#!/usr/bin/env python3
"""
Unpause + trigger DAG Airflow lewat REST API, lalu tunggu sampai selesai.
Exit code 0 kalau DAG run = success, selain itu 1.

Env:
  AIRFLOW_BASE_URL     mis. http://localhost:8080
  AIRFLOW_API_VERSION  v2 (Airflow 3.x, token JWT)  | v1 (Airflow 2.x, basic auth)
  AIRFLOW_USERNAME / AIRFLOW_PASSWORD
  AIRFLOW_API_TOKEN    opsional; kalau diisi dipakai langsung sebagai Bearer
  AIRFLOW_DAG_ID       default etl_oracle_to_clickhouse_mfp
  DAG_WAIT_VISIBLE_S   default 180  (tunggu DAG ter-parse setelah file disalin)
  DAG_TIMEOUT_S        default 3600
"""
import os
import sys
import time

import requests

BASE = os.environ["AIRFLOW_BASE_URL"].rstrip("/")
VER = os.getenv("AIRFLOW_API_VERSION", "v2")
DAG_ID = os.getenv("AIRFLOW_DAG_ID", "etl_oracle_to_clickhouse_mfp")
WAIT_VISIBLE = int(os.getenv("DAG_WAIT_VISIBLE_S", "180"))
TIMEOUT = int(os.getenv("DAG_TIMEOUT_S", "3600"))
API = f"{BASE}/api/{VER}"

s = requests.Session()
s.headers["Content-Type"] = "application/json"


def auth() -> None:
    token = os.getenv("AIRFLOW_API_TOKEN")
    user, pwd = os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    elif VER == "v2":  # Airflow 3: tukar user/password -> JWT
        r = requests.post(f"{BASE}/auth/token", json={"username": user, "password": pwd}, timeout=30)
        r.raise_for_status()
        s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    else:  # Airflow 2: basic auth
        s.auth = (user, pwd)


def log(msg: str) -> None:
    print(f"[airflow] {msg}", flush=True)


def main() -> int:
    auth()

    # 1) tunggu DAG ter-parse
    deadline = time.time() + WAIT_VISIBLE
    while True:
        r = s.get(f"{API}/dags/{DAG_ID}", timeout=30)
        if r.status_code == 200:
            break
        if time.time() > deadline:
            log(f"DAG {DAG_ID} tidak muncul dalam {WAIT_VISIBLE}s (HTTP {r.status_code}).")
            return 1
        log("DAG belum terdeteksi, tunggu 10s...")
        time.sleep(10)

    # 2) unpause
    s.patch(f"{API}/dags/{DAG_ID}", params={"update_mask": "is_paused"},
            json={"is_paused": False}, timeout=30).raise_for_status()

    # 3) trigger
    body = {"conf": {}} if VER == "v1" else {"conf": {}, "logical_date": None}
    r = s.post(f"{API}/dags/{DAG_ID}/dagRuns", json=body, timeout=30)
    r.raise_for_status()
    run_id = r.json()["dag_run_id"]
    log(f"Triggered {DAG_ID} run_id={run_id}")

    # 4) polling status
    deadline = time.time() + TIMEOUT
    last = None
    while time.time() < deadline:
        state = s.get(f"{API}/dags/{DAG_ID}/dagRuns/{run_id}", timeout=30).json().get("state")
        if state != last:
            log(f"state = {state}")
            last = state
        if state == "success":
            return 0
        if state == "failed":
            return 1
        time.sleep(10)
    log("Timeout menunggu DAG selesai.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
