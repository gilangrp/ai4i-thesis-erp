"""
DAG: PostgreSQL -> ClickHouse (Machine Failure Prediction - Incremental Batch).

Schedule: Runs automatically every 1 hour (schedule="@hourly").
Workflow:
1. Read BATCH_SIZE rows (default: 250) from public.machine_operation_staging in PostgreSQL.
2. Append those rows to the ClickHouse table (erp_ai4i.machine_operation).
3. Move the 250 rows in PostgreSQL: copy to public.machine_operation then delete from public.machine_operation_staging.
4. Validate that the data was successfully loaded into ClickHouse.

Required Airflow Connections (Admin > Connections):
  postgres_mfp    Conn Type: Postgres | Host | Port 5432 | Login (airflow_etl) | Password
  clickhouse_mfp  Conn Type: Generic  | Host | Port 8123 (HTTP) | Login | Password
                   Extra: {"secure": false}

PostgreSQL Notes (Hardcoded):
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

# Database, schema, batch & table name configuration (Hardcoded)
PG_DATABASE = "erp_ai4i"
PG_SCHEMA = "public"
CH_DATABASE = "erp_ai4i"
BATCH_SIZE = 250
PG_STAGING_TABLE = f"{PG_SCHEMA}.machine_operation_staging"
PG_MAIN_TABLE = f"{PG_SCHEMA}.machine_operation"
CH_TABLE = "machine_operation"

# (column name, ClickHouse type, Python cast function)
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
    import psycopg2  # PostgreSQL driver

    c = BaseHook.get_connection(PG_CONN_ID)
    host = c.host or "127.0.0.1"
    port = int(c.port or 5432)
    user = c.login or "airflow_etl"
    password = c.password or ""
    dbname = PG_DATABASE
    sslmode = "disable" if host in ("127.0.0.1", "localhost", "::1") else "prefer"

    logger.info(
        ">>> [PG_CONN] Attempting to connect to PostgreSQL at %s:%s (db: %s, user: %s, timeout: %ds, sslmode: %s)...",
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
            logger.info(">>> [PG_CONN] Successfully connected to PostgreSQL (%s:%s/%s).", host, port, dbname)
            return conn
        except concurrent.futures.TimeoutError:
            err_msg = (
                f"PostgreSQL connection TIMED OUT after {timeout} seconds! "
                f"Host: '{host}', Port: {port}, Database: '{dbname}', User: '{user}'. "
                "PostgreSQL may not be responding, port is blocked, or the service is down/hung."
            )
            logger.error(">>> [PG_CONN TIMEOUT] %s", err_msg)
            raise TimeoutError(err_msg)
        except Exception as e:
            logger.error(">>> [PG_CONN ERROR] Failed to connect to PostgreSQL (%s:%s): %s", host, port, str(e))
            raise


def _ch_client(timeout: int = 10):
    import clickhouse_connect
    c = BaseHook.get_connection(CH_CONN_ID)
    extra = json.loads(c.extra) if c.extra else {}
    host = c.host or "127.0.0.1"
    port = int(c.port or 8123)
    user = c.login or "default"
    logger.info(">>> [CH_CONN] Connecting to ClickHouse at %s:%s (db: %s, user: %s)...", host, port, CH_DATABASE, user)
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
    logger.info(">>> [CH_CONN] Successfully connected to ClickHouse.")
    return client


@dag(
    dag_id="etl_postgre_to_clickhouse_mfp",
    description="Incremental ETL: Fetch batch from PostgreSQL Staging, append to ClickHouse, move to main PostgreSQL table",
    schedule="@hourly",  # Run automatically every 1 hour
    start_date=datetime(2026, 1, 1),  # DAG is active starting from this date
    catchup=False,  # Do not backfill missed runs since start_date
    max_active_runs=1,  # Only 1 DAG run allowed at a time to prevent race conditions
    dagrun_timeout=timedelta(hours=1),  # Kill the run if it exceeds 1 hour
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},  # Retry failed tasks up to 2 times with 2-minute delay
    tags=["mfp", "postgres", "clickhouse", "incremental", "staging"],
)
def etl_postgre_to_clickhouse_mfp():

    @task(execution_timeout=timedelta(minutes=2))
    def create_clickhouse_table() -> None:
        """Create the ClickHouse table if it does not exist (MergeTree)."""
        logger.info(">>> [create_clickhouse_table] Checking/creating ClickHouse table '%s'...", CH_TABLE)
        ddl_cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLS)
        ch = _ch_client()
        ch.command(
            f"CREATE TABLE IF NOT EXISTS {CH_TABLE} (\n  {ddl_cols}\n) "
            f"ENGINE = MergeTree ORDER BY udi"
        )
        logger.info(">>> [create_clickhouse_table] ClickHouse table '%s' is ready.", CH_TABLE)

    @task(execution_timeout=timedelta(minutes=5))
    def process_staging_batch() -> dict:
        """
        1. Fetch BATCH_SIZE rows from PostgreSQL machine_operation_staging
        2. Send to ClickHouse (append)
        3. Move data from STAGING to MAIN (machine_operation) in PostgreSQL
        """
        step = "Initializing ClickHouse connection"
        logger.info(">>> ======================================================")
        logger.info(">>> [START] process_staging_batch: Starting ETL batch process")
        logger.info(">>> ======================================================")

        try:
            step = "Connecting to ClickHouse"
            logger.info(">>> [STEP 1/5] %s...", step)
            ch = _ch_client()
            logger.info(">>> [STEP 1/5] ClickHouse connection successful.")

            step = "Connecting to PostgreSQL"
            logger.info(">>> [STEP 2/5] %s...", step)
            conn = _pg_conn()
            logger.info(">>> [STEP 2/5] PostgreSQL connection successful.")

            try:
                with conn.cursor() as cur:
                    # 1. Fetch batch data from staging (ordered by UDI)
                    step = f"Reading data from {PG_STAGING_TABLE} (LIMIT {BATCH_SIZE})"
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
                        logger.info(">>> [INFO] Table %s is empty. No data to process.", PG_STAGING_TABLE)
                        return {"count": 0, "sample_udis": []}

                    batch_count = len(rows)
                    udis = [r[0] for r in rows]
                    logger.info(
                        ">>> [STEP 2/5] Successfully fetched %d rows from %s (UDI range: %d .. %d).",
                        batch_count, PG_STAGING_TABLE, udis[0], udis[-1]
                    )

                    # 2. Send data to ClickHouse (append)
                    step = f"Insert {batch_count} rows into ClickHouse ({CH_TABLE})"
                    logger.info(">>> [STEP 3/5] %s...", step)
                    data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                            for r in rows]
                    ch.insert(CH_TABLE, data, column_names=COL_NAMES)
                    logger.info(">>> [STEP 3/5] Successfully inserted %d rows into ClickHouse (%s).", batch_count, CH_TABLE)

                    # 3. Move to machine_operation in PostgreSQL
                    step = f"Insert {batch_count} rows into PostgreSQL ({PG_MAIN_TABLE})"
                    logger.info(">>> [STEP 4/5] %s...", step)
                    placeholders = ", ".join(["%s"] * len(COL_NAMES))
                    insert_main_sql = f"""
                        INSERT INTO {PG_MAIN_TABLE} ({cols_str})
                        VALUES ({placeholders})
                    """
                    cur.executemany(insert_main_sql, rows)
                    logger.info(">>> [STEP 4/5] Successfully copied %d rows to %s.", batch_count, PG_MAIN_TABLE)

                    # 4. Delete the moved rows from STAGING in PostgreSQL
                    step = f"Deleting {batch_count} rows from PostgreSQL ({PG_STAGING_TABLE})"
                    logger.info(">>> [STEP 5/5] %s...", step)
                    delete_staging_sql = f"DELETE FROM {PG_STAGING_TABLE} WHERE udi = %s"
                    cur.executemany(delete_staging_sql, [[u] for u in udis])
                    logger.info(">>> [STEP 5/5] Successfully deleted %d rows from %s.", batch_count, PG_STAGING_TABLE)

                    step = "Committing PostgreSQL transaction"
                    logger.info(">>> [STEP 5/5] Committing PostgreSQL transaction...")
                    conn.commit()
                    logger.info(">>> [SUCCESS] PostgreSQL transaction committed successfully.")

                    return {
                        "count": batch_count,
                        "first_udi": udis[0],
                        "last_udi": udis[-1]
                    }

            except Exception as e_db:
                logger.error(">>> [DATABASE ERROR] Error occurred at step: '%s'. Details: %s", step, str(e_db))
                try:
                    conn.rollback()
                    logger.warning(">>> [ROLLBACK] PostgreSQL transaction rolled back successfully to release locks.")
                except Exception as e_rb:
                    logger.error(">>> [ROLLBACK ERROR] Failed to rollback: %s", str(e_rb))
                raise e_db

            finally:
                try:
                    conn.close()
                    logger.info(">>> [CONNECTION] PostgreSQL connection closed successfully.")
                except Exception:
                    pass

        except Exception as e:
            logger.exception(">>> [FATAL ERROR] Failed at step: '%s'. Exception: %s", step, str(e))
            raise

    @task(execution_timeout=timedelta(minutes=2))
    def validate(result: dict) -> None:
        """Validate the data that was just loaded."""
        count = result.get("count", 0)
        if count == 0:
            logger.info(">>> [validate] No batch was loaded in this run.")
            return

        first_udi = result["first_udi"]
        last_udi = result["last_udi"]

        logger.info(">>> [validate] Validating ClickHouse data for UDI range %d..%d (expecting >= %d rows)...", first_udi, last_udi, count)
        ch = _ch_client()
        ch_check = ch.query(
            f"SELECT count() FROM {CH_TABLE} WHERE udi >= {first_udi} AND udi <= {last_udi}"
        ).result_rows[0][0]

        logger.info(">>> [validate] ClickHouse check result: %s rows found.", ch_check)
        if int(ch_check) < count:
            err_msg = f"Validation failed: expected at least {count} rows in ClickHouse, but found {ch_check}"
            logger.error(">>> [validate ERROR] %s", err_msg)
            raise ValueError(err_msg)
        logger.info(">>> [validate SUCCESS] Validation passed, all data loaded successfully.")

    create_clickhouse_table() >> validate(process_staging_batch())


etl_postgre_to_clickhouse_mfp()
