"""
DAG: Oracle 26ai -> ClickHouse (Machine Failure Prediction - Incremental Batch).

Schedule: Runs automatically every 1 hour (schedule="@hourly").
Workflow:
1. Read BATCH_SIZE rows (default: 250) from MACHINE_OPERATION_STAGING in Oracle.
2. Append those rows to the ClickHouse table (machine_operation).
3. Move the 250 rows in Oracle: copy to MACHINE_OPERATION then delete from MACHINE_OPERATION_STAGING.
4. Validate that the data was successfully loaded into ClickHouse.

Required Airflow Connections (Admin > Connections):
  oracle_mfp      Conn Type: Generic | Host | Port 1521 | Login | Password
                   Extra: {"service_name": "FREEPDB1"}
  clickhouse_mfp  Conn Type: Generic | Host | Port 8123 (HTTP) | Login | Password
                   Schema: <ClickHouse database name>   Extra: {"secure": false}

Env (optional):
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

# Batch & table name configuration
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
    import oracledb  # thin mode, no Oracle Client required
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
    description="Incremental ETL: Fetch batch from Oracle Staging, append to ClickHouse, move to main Oracle table",
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
        """Create the ClickHouse table if it does not exist (MergeTree)."""
        ddl_cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLS)
        ch = _ch_client()
        ch.command(
            f"CREATE TABLE IF NOT EXISTS {CH_TABLE} (\n  {ddl_cols}\n) "
            f"ENGINE = MergeTree ORDER BY udi"
        )

    @task
    def process_staging_batch() -> dict:
        """
        1. Fetch BATCH_SIZE rows from Oracle MACHINE_OPERATION_STAGING
        2. Send to ClickHouse (append)
        3. Move data from STAGING to MAIN (MACHINE_OPERATION) in Oracle
        """
        ch = _ch_client()

        with _oracle_conn() as conn:
            cur = conn.cursor()

            # 1. Fetch batch data from staging (ordered by UDI)
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
                print(f"Table {ORACLE_STAGING_TABLE} is empty. No data to process.")
                return {"count": 0, "sample_udis": []}

            batch_count = len(rows)
            udis = [r[0] for r in rows]
            print(f"Fetched {batch_count} rows from {ORACLE_STAGING_TABLE} (UDI: {udis[0]} .. {udis[-1]}).")

            # 2. Send data to ClickHouse (append)
            data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                    for r in rows]
            ch.insert(CH_TABLE, data, column_names=COL_NAMES)
            print(f"Successfully inserted {batch_count} rows into ClickHouse ({CH_TABLE}).")

            # 3. Move to MACHINE_OPERATION in Oracle
            placeholders = ", ".join([f":{i+1}" for i in range(len(COL_NAMES))])
            insert_main_sql = f"""
                INSERT INTO {ORACLE_MAIN_TABLE} ({cols_str})
                VALUES ({placeholders})
            """
            cur.executemany(insert_main_sql, rows)

            # 4. Delete the moved rows from STAGING in Oracle
            delete_staging_sql = f"DELETE FROM {ORACLE_STAGING_TABLE} WHERE udi = :1"
            cur.executemany(delete_staging_sql, [[u] for u in udis])

            conn.commit()
            print(f"Successfully moved {batch_count} rows from {ORACLE_STAGING_TABLE} to {ORACLE_MAIN_TABLE}.")

            return {
                "count": batch_count,
                "first_udi": udis[0],
                "last_udi": udis[-1]
            }

    @task
    def validate(result: dict) -> None:
        """Validate the data that was just loaded."""
        count = result.get("count", 0)
        if count == 0:
            print("No batch was loaded in this run.")
            return

        first_udi = result["first_udi"]
        last_udi = result["last_udi"]

        # Verify data exists in ClickHouse
        ch = _ch_client()
        ch_check = ch.query(
            f"SELECT count() FROM {CH_TABLE} WHERE udi >= {first_udi} AND udi <= {last_udi}"
        ).result_rows[0][0]

        print(f"ClickHouse validation: {ch_check} rows found for UDI range {first_udi}..{last_udi}")
        if int(ch_check) < count:
            raise ValueError(f"Validation failed: expected at least {count} rows in ClickHouse, but found {ch_check}")

    create_clickhouse_table() >> validate(process_staging_batch())


etl_oracle_to_clickhouse_mfp()
