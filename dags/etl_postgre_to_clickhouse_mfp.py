"""
DAG: PostgreSQL -> ClickHouse (Machine Failure Prediction - Incremental Batch).

Jadwal: Berjalan otomatis setiap 1 jam sekali (schedule="@hourly").
Alur Kerja:
1. Membaca data sejumlah BATCH_SIZE (default: 250) dari machine_operation_staging di PostgreSQL.
2. Mengirim (append) data tersebut ke tabel ClickHouse (machine_operation).
3. Memindahkan 250 data tersebut di PostgreSQL: disalin ke machine_operation lalu dihapus dari machine_operation_staging.
4. Validasi data berhasil termuat di ClickHouse.

Airflow Connections yang dibutuhkan (Admin > Connections):
  postgres_mfp    Conn Type: Postgres | Host | Port 5432 | Login (airflow_etl) | Password | Schema / Database: erp_ai4i
  clickhouse_mfp  Conn Type: Generic  | Host | Port 8123 (HTTP) | Login | Password
                   Schema: erp_ai4i   Extra: {"secure": false}

Catatan PostgreSQL:
  - Database: erp_ai4i
  - Schema: public (tabel: public.machine_operation & public.machine_operation_staging)
"""
import json
from datetime import datetime, timedelta

from airflow.sdk import BaseHook, dag, task

PG_CONN_ID = "postgres_mfp"
CH_CONN_ID = "clickhouse_mfp"

# Konfigurasi database, batch & nama tabel
PG_DATABASE = "erp_ai4i"
CH_DATABASE = "erp_ai4i"
BATCH_SIZE = 250
PG_STAGING_TABLE = "MACHINE_OPERATION_STAGING"
PG_MAIN_TABLE = "MACHINE_OPERATION"
CH_TABLE = "machine_operation"

# (nama kolom, tipe ClickHouse, fungsi cast Python)
COLS = [
    ("udi", "UInt32", int), ("product_id", "String", str),
    ("type", "LowCardinality(String)", str),
    ("air_temp_k", "Float64", float), ("process_temp_k", "Float64", float),
    ("rpm", "UInt32", int), ("torque_nm", "Float64", float),
    ("tool_wear_min", "UInt32", int), ("machine_failure", "UInt8", int),
    ("twf", "UInt8", int), ("hdf", "UInt8", int), ("pwf", "UInt8", int),
    ("osf", "UInt8", int), ("rnf", "UInt8", int),
]
COL_NAMES = [c[0] for c in COLS]


def _pg_conn():
    import psycopg2  # driver PostgreSQL
    c = BaseHook.get_connection(PG_CONN_ID)
    # Catatan: Di PostgreSQL, 'public' adalah schema, sedangkan database adalah 'erp_ai4i'.
    # Jika c.schema kosong atau bernilai 'public' / 'postgres', gunakan database 'erp_ai4i'.
    dbname = c.schema if c.schema and c.schema not in ("public", "postgres") else PG_DATABASE
    return psycopg2.connect(
        host=c.host,
        port=c.port or 5432,
        user=c.login or "airflow_etl",
        password=c.password or "",
        dbname=dbname,
    )


def _ch_client():
    import clickhouse_connect
    c = BaseHook.get_connection(CH_CONN_ID)
    extra = json.loads(c.extra) if c.extra else {}
    return clickhouse_connect.get_client(
        host=c.host, port=int(c.port or 8123), username=c.login or "default",
        password=c.password or "", database=c.schema or CH_DATABASE,
        secure=bool(extra.get("secure", False)),
    )


@dag(
    dag_id="etl_postgre_to_clickhouse_mfp",
    description="Incremental ETL: Ambil batch dari Staging PostgreSQL, append ke ClickHouse, pindah ke main PostgreSQL",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["mfp", "postgres", "clickhouse", "incremental", "staging"],
)
def etl_postgre_to_clickhouse_mfp():

    @task
    def create_clickhouse_table() -> None:
        """Membuat tabel ClickHouse jika belum ada (MergeTree)."""
        ddl_cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLS)
        ch = _ch_client()
        ch.command(
            f"CREATE TABLE IF NOT EXISTS {CH_TABLE} (\n  {ddl_cols}\n) "
            f"ENGINE = MergeTree ORDER BY udi"
        )

    @task
    def process_staging_batch() -> dict:
        """
        1. Ambil data sebanyak BATCH_SIZE dari PostgreSQL machine_operation_staging
        2. Kirim ke ClickHouse (append)
        3. Pindahkan data dari STAGING ke MAIN (machine_operation) di PostgreSQL
        """
        ch = _ch_client()

        with _pg_conn() as conn:
            with conn.cursor() as cur:
                # 1. Ambil data batch dari staging (urut UDI)
                cols_str = ", ".join(COL_NAMES)
                fetch_query = f"""
                    SELECT {cols_str}
                    FROM {PG_STAGING_TABLE}
                    ORDER BY udi
                    LIMIT %s
                """
                cur.execute(fetch_query, (BATCH_SIZE,))
                rows = cur.fetchall()

                if not rows:
                    print(f"Tabel {PG_STAGING_TABLE} kosong. Tidak ada data untuk diproses.")
                    return {"count": 0, "sample_udis": []}

                batch_count = len(rows)
                udis = [r[0] for r in rows]
                print(f"Mengambil {batch_count} baris dari {PG_STAGING_TABLE} (UDI: {udis[0]} .. {udis[-1]}).")

                # 2. Kirim data ke ClickHouse (append)
                data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                        for r in rows]
                ch.insert(CH_TABLE, data, column_names=COL_NAMES)
                print(f"Berhasil meng-insert {batch_count} baris ke ClickHouse ({CH_TABLE}).")

                # 3. Pindahkan ke machine_operation di PostgreSQL
                placeholders = ", ".join(["%s"] * len(COL_NAMES))
                insert_main_sql = f"""
                    INSERT INTO {PG_MAIN_TABLE} ({cols_str})
                    VALUES ({placeholders})
                """
                cur.executemany(insert_main_sql, rows)

                # 4. Hapus data yang sudah dipindahkan dari STAGING di PostgreSQL
                delete_staging_sql = f"DELETE FROM {PG_STAGING_TABLE} WHERE udi = %s"
                cur.executemany(delete_staging_sql, [[u] for u in udis])

                conn.commit()
                print(f"Berhasil memindahkan {batch_count} baris dari {PG_STAGING_TABLE} ke {PG_MAIN_TABLE}.")

                return {
                    "count": batch_count,
                    "first_udi": udis[0],
                    "last_udi": udis[-1]
                }

    @task
    def validate(result: dict) -> None:
        """Validasi data yang baru saja dipindahkan."""
        count = result.get("count", 0)
        if count == 0:
            print("Tidak ada batch yang dimuat pada run kali ini.")
            return

        first_udi = result["first_udi"]
        last_udi = result["last_udi"]

        # Verifikasi data ada di ClickHouse
        ch = _ch_client()
        ch_check = ch.query(
            f"SELECT count() FROM {CH_TABLE} WHERE udi >= {first_udi} AND udi <= {last_udi}"
        ).result_rows[0][0]

        print(f"Validasi ClickHouse: {ch_check} baris ditemukan untuk rentang UDI {first_udi}..{last_udi}")
        if int(ch_check) < count:
            raise ValueError(f"Validasi gagal: diharapkan minimal {count} baris di ClickHouse, tapi ada {ch_check}")

    create_clickhouse_table() >> validate(process_staging_batch())


etl_postgre_to_clickhouse_mfp()
