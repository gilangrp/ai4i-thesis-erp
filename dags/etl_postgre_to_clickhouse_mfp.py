"""
DAG: PostgreSQL -> ClickHouse (Machine Failure Prediction - Incremental Batch).

Jadwal: Berjalan otomatis setiap 1 jam sekali (schedule="@hourly").
Alur Kerja:
1. Membaca data sejumlah BATCH_SIZE (default: 250) dari public.machine_operation_staging di PostgreSQL.
2. Mengirim (append) data tersebut ke tabel ClickHouse (erp_ai4i.machine_operation).
3. Memindahkan 250 data tersebut di PostgreSQL: disalin ke public.machine_operation lalu dihapus dari public.machine_operation_staging.
4. Validasi data berhasil termuat di ClickHouse.

Airflow Connections yang dibutuhkan (Admin > Connections):
  postgres_mfp    Conn Type: Postgres | Host | Port 5432 | Login (airflow_etl) | Password
  clickhouse_mfp  Conn Type: Generic  | Host | Port 8123 (HTTP) | Login | Password
                   Extra: {"secure": false}

Catatan PostgreSQL (Hardcoded):
  - Database: erp_ai4i
  - Schema: public
  - Tables: public.machine_operation_staging & public.machine_operation
"""
import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import BaseHook, dag, task

logger = logging.getLogger("airflow.task")

PG_CONN_ID = "postgres_mfp"
CH_CONN_ID = "clickhouse_mfp"

# Konfigurasi database, schema, batch & nama tabel (Hardcoded)
PG_DATABASE = "erp_ai4i"
PG_SCHEMA = "public"
CH_DATABASE = "erp_ai4i"
BATCH_SIZE = 250
PG_STAGING_TABLE = f"{PG_SCHEMA}.machine_operation_staging"
PG_MAIN_TABLE = f"{PG_SCHEMA}.machine_operation"
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


def _pg_conn(timeout: int = 10):
    import concurrent.futures
    import psycopg2  # driver PostgreSQL

    c = BaseHook.get_connection(PG_CONN_ID)
    host = c.host or "127.0.0.1"
    port = int(c.port or 5432)
    user = c.login or "airflow_etl"
    password = c.password or ""
    dbname = PG_DATABASE
    sslmode = "disable" if host in ("127.0.0.1", "localhost", "::1") else "prefer"

    logger.info(
        ">>> [PG_CONN] Mencoba koneksi ke PostgreSQL di %s:%s (db: %s, user: %s, timeout: %ds, sslmode: %s)...",
        host, port, dbname, user, timeout, sslmode
    )

    def _do_connect():
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            sslmode=sslmode,
            connect_timeout=timeout,
            options=f"-c search_path={PG_SCHEMA} -c lock_timeout=15000 -c statement_timeout=30000",
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_do_connect)
        try:
            conn = future.result(timeout=timeout)
            logger.info(">>> [PG_CONN] Berhasil terhubung ke PostgreSQL (%s:%s/%s).", host, port, dbname)
            return conn
        except concurrent.futures.TimeoutError:
            err_msg = (
                f"Koneksi ke PostgreSQL TIMEOUT setelah {timeout} detik! "
                f"Host: '{host}', Port: {port}, Database: '{dbname}', User: '{user}'. "
                "Kemungkinan PostgreSQL tidak merespons, port terblokir, atau service mati/hang."
            )
            logger.error(">>> [PG_CONN TIMEOUT] %s", err_msg)
            raise TimeoutError(err_msg)
        except Exception as e:
            logger.error(">>> [PG_CONN ERROR] Gagal terhubung ke PostgreSQL (%s:%s): %s", host, port, str(e))
            raise


def _ch_client(timeout: int = 10):
    import clickhouse_connect
    c = BaseHook.get_connection(CH_CONN_ID)
    extra = json.loads(c.extra) if c.extra else {}
    host = c.host or "127.0.0.1"
    port = int(c.port or 8123)
    user = c.login or "default"
    logger.info(">>> [CH_CONN] Menghubungkan ke ClickHouse di %s:%s (db: %s, user: %s)...", host, port, CH_DATABASE, user)
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=c.password or "",
        database=CH_DATABASE,
        secure=bool(extra.get("secure", False)),
        connect_timeout=timeout,
        send_receive_timeout=30,
    )
    logger.info(">>> [CH_CONN] Berhasil terhubung ke ClickHouse.")
    return client


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

    @task(execution_timeout=timedelta(minutes=2))
    def create_clickhouse_table() -> None:
        """Membuat tabel ClickHouse jika belum ada (MergeTree)."""
        logger.info(">>> [create_clickhouse_table] Memeriksa/membuat tabel ClickHouse '%s'...", CH_TABLE)
        ddl_cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLS)
        ch = _ch_client()
        ch.command(
            f"CREATE TABLE IF NOT EXISTS {CH_TABLE} (\n  {ddl_cols}\n) "
            f"ENGINE = MergeTree ORDER BY udi"
        )
        logger.info(">>> [create_clickhouse_table] Tabel ClickHouse '%s' siap digunakan.", CH_TABLE)

    @task(execution_timeout=timedelta(minutes=5))
    def process_staging_batch() -> dict:
        """
        1. Ambil data sebanyak BATCH_SIZE dari PostgreSQL machine_operation_staging
        2. Kirim ke ClickHouse (append)
        3. Pindahkan data dari STAGING ke MAIN (machine_operation) di PostgreSQL
        """
        step = "Inisialisasi koneksi ClickHouse"
        logger.info(">>> ======================================================")
        logger.info(">>> [MULAI] process_staging_batch: Memulai proses batch ETL")
        logger.info(">>> ======================================================")

        try:
            step = "Menghubungkan ke ClickHouse"
            logger.info(">>> [STEP 1/5] %s...", step)
            ch = _ch_client()
            logger.info(">>> [STEP 1/5] Koneksi ke ClickHouse berhasil.")

            step = "Menghubungkan ke PostgreSQL"
            logger.info(">>> [STEP 2/5] %s...", step)
            conn = _pg_conn()
            logger.info(">>> [STEP 2/5] Koneksi ke PostgreSQL berhasil.")

            try:
                with conn.cursor() as cur:
                    # 1. Ambil data batch dari staging (urut UDI)
                    step = f"Membaca data dari {PG_STAGING_TABLE} (LIMIT {BATCH_SIZE})"
                    logger.info(">>> [STEP 2/5] %s...", step)
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
                        logger.info(">>> [INFO] Tabel %s kosong. Tidak ada data untuk diproses.", PG_STAGING_TABLE)
                        return {"count": 0, "sample_udis": []}

                    batch_count = len(rows)
                    udis = [r[0] for r in rows]
                    logger.info(
                        ">>> [STEP 2/5] Berhasil mengambil %d baris dari %s (Rentang UDI: %d .. %d).",
                        batch_count, PG_STAGING_TABLE, udis[0], udis[-1]
                    )

                    # 2. Kirim data ke ClickHouse (append)
                    step = f"Insert {batch_count} baris ke ClickHouse ({CH_TABLE})"
                    logger.info(">>> [STEP 3/5] %s...", step)
                    data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                            for r in rows]
                    ch.insert(CH_TABLE, data, column_names=COL_NAMES)
                    logger.info(">>> [STEP 3/5] Berhasil meng-insert %d baris ke ClickHouse (%s).", batch_count, CH_TABLE)

                    # 3. Pindahkan ke machine_operation di PostgreSQL
                    step = f"Insert {batch_count} baris ke PostgreSQL ({PG_MAIN_TABLE})"
                    logger.info(">>> [STEP 4/5] %s...", step)
                    placeholders = ", ".join(["%s"] * len(COL_NAMES))
                    insert_main_sql = f"""
                        INSERT INTO {PG_MAIN_TABLE} ({cols_str})
                        VALUES ({placeholders})
                    """
                    cur.executemany(insert_main_sql, rows)
                    logger.info(">>> [STEP 4/5] Berhasil menyalin %d baris ke %s.", batch_count, PG_MAIN_TABLE)

                    # 4. Hapus data yang sudah dipindahkan dari STAGING di PostgreSQL
                    step = f"Menghapus {batch_count} baris dari PostgreSQL ({PG_STAGING_TABLE})"
                    logger.info(">>> [STEP 5/5] %s...", step)
                    delete_staging_sql = f"DELETE FROM {PG_STAGING_TABLE} WHERE udi = %s"
                    cur.executemany(delete_staging_sql, [[u] for u in udis])
                    logger.info(">>> [STEP 5/5] Berhasil menghapus %d baris dari %s.", batch_count, PG_STAGING_TABLE)

                    step = "Commit transaksi PostgreSQL"
                    logger.info(">>> [STEP 5/5] Melakukan commit transaksi PostgreSQL...")
                    conn.commit()
                    logger.info(">>> [SUKSES] Transaksi PostgreSQL berhasil di-commit sepenuhnya.")

                    return {
                        "count": batch_count,
                        "first_udi": udis[0],
                        "last_udi": udis[-1]
                    }

            except Exception as e_db:
                logger.error(">>> [ERROR DATABASE] Terjadi error pada tahap: '%s'. Detail: %s", step, str(e_db))
                try:
                    conn.rollback()
                    logger.warning(">>> [ROLLBACK] Rollback transaksi PostgreSQL berhasil dijalankan untuk melepas lock.")
                except Exception as e_rb:
                    logger.error(">>> [ERROR ROLLBACK] Gagal melakukan rollback: %s", str(e_rb))
                raise e_db

            finally:
                try:
                    conn.close()
                    logger.info(">>> [KONEKSI] Koneksi PostgreSQL berhasil ditutup.")
                except Exception:
                    pass

        except Exception as e:
            logger.exception(">>> [FATAL ERROR] Gagal pada tahap: '%s'. Exception: %s", step, str(e))
            raise

    @task(execution_timeout=timedelta(minutes=2))
    def validate(result: dict) -> None:
        """Validasi data yang baru saja dipindahkan."""
        count = result.get("count", 0)
        if count == 0:
            logger.info(">>> [validate] Tidak ada batch yang dimuat pada run kali ini.")
            return

        first_udi = result["first_udi"]
        last_udi = result["last_udi"]

        logger.info(">>> [validate] Memvalidasi data ClickHouse untuk rentang UDI %d..%d (diharapkan >= %d baris)...", first_udi, last_udi, count)
        ch = _ch_client()
        ch_check = ch.query(
            f"SELECT count() FROM {CH_TABLE} WHERE udi >= {first_udi} AND udi <= {last_udi}"
        ).result_rows[0][0]

        logger.info(">>> [validate] Hasil pengecekan ClickHouse: %s baris ditemukan.", ch_check)
        if int(ch_check) < count:
            err_msg = f"Validasi gagal: diharapkan minimal {count} baris di ClickHouse, tapi ada {ch_check}"
            logger.error(">>> [validate ERROR] %s", err_msg)
            raise ValueError(err_msg)
        logger.info(">>> [validate SUKSES] Validasi berhasil, semua data termuat.")

    create_clickhouse_table() >> validate(process_staging_batch())


etl_postgre_to_clickhouse_mfp()
