"""
DAG: Oracle 26ai -> ClickHouse (Machine Failure Prediction - Incremental Batch).

Jadwal: Berjalan otomatis setiap 1 jam sekali (schedule="@hourly").
Alur Kerja:
1. Membaca data sejumlah BATCH_SIZE (default: 250) dari MACHINE_OPERATION_STAGING di Oracle.
2. Mengirim (append) data tersebut ke tabel ClickHouse (machine_operation).
3. Memindahkan 250 data tersebut di Oracle: disalin ke MACHINE_OPERATION lalu dihapus dari MACHINE_OPERATION_STAGING.
4. Validasi data berhasil termuat di ClickHouse.

Airflow Connections yang dibutuhkan (Admin > Connections):
  oracle_mfp      Conn Type: Generic | Host | Port 1521 | Login | Password
                   Extra: {"service_name": "FREEPDB1"}
  clickhouse_mfp  Conn Type: Generic | Host | Port 8123 (HTTP) | Login | Password
                   Schema: <nama database ClickHouse>   Extra: {"secure": false}

Env (opsional):
  STAGING_BATCH_SIZE    default 250
  ORACLE_STAGING_TABLE  default MACHINE_OPERATION_STAGING
  ORACLE_MAIN_TABLE     default MACHINE_OPERATION
  CH_TABLE              default machine_operation
"""
import json
import os
from datetime import datetime, timedelta

try:  # Airflow 3.x
    from airflow.sdk import BaseHook, dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.hooks.base import BaseHook

ORACLE_CONN_ID = "oracle_mfp"
CH_CONN_ID = "clickhouse_mfp"

# Konfigurasi batch & nama tabel
BATCH_SIZE = int(os.getenv("STAGING_BATCH_SIZE", "250"))
ORACLE_STAGING_TABLE = os.getenv("ORACLE_STAGING_TABLE", "MACHINE_OPERATION_STAGING")
ORACLE_MAIN_TABLE = os.getenv("ORACLE_MAIN_TABLE", "MACHINE_OPERATION")
CH_TABLE = os.getenv("CH_TABLE", "machine_operation")

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


def _oracle_conn():
    import oracledb  # thin mode, tidak perlu Oracle Client
    c = BaseHook.get_connection(ORACLE_CONN_ID)
    extra = json.loads(c.extra) if c.extra else {}
    service = extra.get("service_name") or c.schema
    dsn = f"{c.host}:{c.port or 1521}/{service}"
    return oracledb.connect(user=c.login, password=c.password, dsn=dsn)


def _ch_client():
    import clickhouse_connect
    c = BaseHook.get_connection(CH_CONN_ID)
    extra = json.loads(c.extra) if c.extra else {}
    return clickhouse_connect.get_client(
        host=c.host, port=int(c.port or 8123), username=c.login or "default",
        password=c.password or "", database=c.schema or "default",
        secure=bool(extra.get("secure", False)),
    )


@dag(
    dag_id="etl_oracle_to_clickhouse_mfp",
    description="Incremental ETL: Ambil batch dari Staging Oracle, append ke ClickHouse, pindah ke main Oracle",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["mfp", "oracle", "clickhouse", "incremental", "staging"],
)
def etl_oracle_to_clickhouse_mfp():

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
        1. Ambil data sebanyak BATCH_SIZE dari Oracle MACHINE_OPERATION_STAGING
        2. Kirim ke ClickHouse (append)
        3. Pindahkan data dari STAGING ke MAIN (MACHINE_OPERATION) di Oracle
        """
        ch = _ch_client()

        with _oracle_conn() as conn:
            cur = conn.cursor()

            # 1. Ambil data batch dari staging (urut UDI)
            cols_str = ", ".join(COL_NAMES)
            fetch_query = f"""
                SELECT {cols_str}
                FROM {ORACLE_STAGING_TABLE}
                ORDER BY udi
                FETCH FIRST :batch_size ROWS ONLY
            """
            cur.execute(fetch_query, [BATCH_SIZE])
            rows = cur.fetchall()

            if not rows:
                print(f"Tabel {ORACLE_STAGING_TABLE} kosong. Tidak ada data untuk diproses.")
                return {"count": 0, "sample_udis": []}

            batch_count = len(rows)
            udis = [r[0] for r in rows]
            print(f"Mengambil {batch_count} baris dari {ORACLE_STAGING_TABLE} (UDI: {udis[0]} .. {udis[-1]}).")

            # 2. Kirim data ke ClickHouse (append)
            data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                    for r in rows]
            ch.insert(CH_TABLE, data, column_names=COL_NAMES)
            print(f"Berhasil meng-insert {batch_count} baris ke ClickHouse ({CH_TABLE}).")

            # 3. Pindahkan ke MACHINE_OPERATION di Oracle
            placeholders = ", ".join([f":{i+1}" for i in range(len(COL_NAMES))])
            insert_main_sql = f"""
                INSERT INTO {ORACLE_MAIN_TABLE} ({cols_str})
                VALUES ({placeholders})
            """
            cur.executemany(insert_main_sql, rows)

            # 4. Hapus data yang sudah dipindahkan dari STAGING di Oracle
            delete_staging_sql = f"DELETE FROM {ORACLE_STAGING_TABLE} WHERE udi = :1"
            cur.executemany(delete_staging_sql, [[u] for u in udis])

            conn.commit()
            print(f"Berhasil memindahkan {batch_count} baris dari {ORACLE_STAGING_TABLE} ke {ORACLE_MAIN_TABLE}.")

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


etl_oracle_to_clickhouse_mfp()
