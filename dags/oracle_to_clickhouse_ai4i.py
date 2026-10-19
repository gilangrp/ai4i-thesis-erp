"""
DAG: Oracle 26ai -> ClickHouse (dataset AI4I 2020).

Dipicu lewat API (schedule=None) oleh GitHub Actions / deploy script.
Full refresh: tabel ClickHouse dikosongkan lalu diisi ulang dari Oracle,
kemudian divalidasi (jumlah baris + jumlah failure harus sama).

Airflow Connections yang dibutuhkan (Admin > Connections):
  oracle_ai4i      Conn Type: Generic | Host | Port 1521 | Login | Password
                   Extra: {"service_name": "FREEPDB1"}
  clickhouse_ai4i  Conn Type: Generic | Host | Port 8123 (HTTP) | Login | Password
                   Schema: <nama database ClickHouse>   Extra: {"secure": false}

Env (opsional, default di bawah):
  ORACLE_SOURCE_TABLE   default AI4I_2020   (boleh "SCHEMA.TABEL")
  CH_TABLE              default ai4i_2020
Dependensi di environment Airflow: oracledb, clickhouse-connect
"""
import json
import os
from datetime import datetime, timedelta

try:  # Airflow 3.x
    from airflow.sdk import BaseHook, dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.hooks.base import BaseHook

ORACLE_CONN_ID = "oracle_ai4i"
CH_CONN_ID = "clickhouse_ai4i"
ORACLE_TABLE = os.getenv("ORACLE_SOURCE_TABLE", "AI4I_2020")
CH_TABLE = os.getenv("CH_TABLE", "ai4i_2020")
CHUNK = 5000

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
    dag_id="oracle_to_clickhouse_ai4i",
    description="Full refresh dataset AI4I 2020 dari Oracle ke ClickHouse",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ai4i", "oracle", "clickhouse", "mlops"],
)
def oracle_to_clickhouse_ai4i():

    @task
    def create_clickhouse_table() -> None:
        ddl_cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLS)
        ch = _ch_client()
        ch.command(
            f"CREATE TABLE IF NOT EXISTS {CH_TABLE} (\n  {ddl_cols}\n) "
            f"ENGINE = MergeTree ORDER BY udi"
        )

    @task
    def extract_and_load() -> int:
        ch = _ch_client()
        ch.command(f"TRUNCATE TABLE {CH_TABLE}")  # full refresh
        total = 0
        with _oracle_conn() as conn:
            cur = conn.cursor()
            cur.arraysize = CHUNK
            cur.execute(f"SELECT {', '.join(COL_NAMES)} FROM {ORACLE_TABLE} ORDER BY udi")
            while True:
                rows = cur.fetchmany(CHUNK)
                if not rows:
                    break
                data = [[fn(v) if v is not None else None for v, (_, _, fn) in zip(r, COLS)]
                        for r in rows]
                ch.insert(CH_TABLE, data, column_names=COL_NAMES)
                total += len(data)
        print(f"Loaded {total} rows into {CH_TABLE}")
        return total

    @task
    def validate(loaded: int) -> None:
        with _oracle_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*), NVL(SUM(machine_failure), 0) FROM {ORACLE_TABLE}")
            ora_count, ora_fail = (int(x) for x in cur.fetchone())
        ch_count, ch_fail = _ch_client().query(
            f"SELECT count(), sum(machine_failure) FROM {CH_TABLE}").result_rows[0]
        print(f"Oracle: {ora_count} baris / {ora_fail} failure | ClickHouse: {ch_count} / {ch_fail}")
        if ora_count == 0:
            raise ValueError("Tabel Oracle kosong, load dibatalkan.")
        if (ora_count, ora_fail) != (int(ch_count), int(ch_fail)) or loaded != ora_count:
            raise ValueError("Validasi gagal: data Oracle dan ClickHouse tidak sama.")

    create_clickhouse_table() >> validate(extract_and_load())


oracle_to_clickhouse_ai4i()
