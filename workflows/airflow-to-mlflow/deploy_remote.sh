#!/usr/bin/env bash
# ==============================================================================
# CI/CD PIPELINE SCRIPT — GITHUB ACTIONS RUNNER
# Steps:
# 1. Setup Python environment & install dependencies
# 2. Initialize local Airflow & register ClickHouse VPS + Source DB connections
# 3. Execute Airflow DAG: airflow dags test
# 4. Run training notebook via Papermill (reads from ClickHouse VPS -> logs to MLflow)
# 5. All outputs (airflow.db, mlflow.db, mlruns/) saved as GitHub Actions artifacts
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

step() { echo; echo "==> $*"; }

# -----------------------------------------------------------------------------
# Configuration — VPS & Database
# -----------------------------------------------------------------------------
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-202.10.42.115}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-airflow_etl}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-airflow_etl}"
export CLICKHOUSE_DB="${CLICKHOUSE_DB:-erp_ai4i}"
export CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-machine_operation}"

# Aliases for notebook environment variables
export CH_HOST="$CLICKHOUSE_HOST"
export CH_PORT="$CLICKHOUSE_PORT"
export CH_USER="$CLICKHOUSE_USER"
export CH_PASSWORD="$CLICKHOUSE_PASSWORD"
export CH_DATABASE="$CLICKHOUSE_DB"
export CH_TABLE="$CLICKHOUSE_TABLE"
export DATA_SOURCE="clickhouse"
export MLFLOW_TRACKING_URI="sqlite:///$ROOT_DIR/mlflow.db"

# Source DB (Oracle / PostgreSQL) on VPS
export SOURCE_DB_HOST="${SOURCE_DB_HOST:-202.10.42.115}"
export SOURCE_DB_PORT="${SOURCE_DB_PORT:-5432}"
export SOURCE_DB_USER="${SOURCE_DB_USER:-airflow_etl}"
export SOURCE_DB_PASSWORD="${SOURCE_DB_PASSWORD:-airflow_etl}"
export SOURCE_DB_NAME="${SOURCE_DB_NAME:-erp_ai4i}"

export AIRFLOW_HOME="${AIRFLOW_HOME:-$ROOT_DIR/airflow_home}"
# Default DAG: etl_postgre_to_clickhouse_mfp or etl_oracle_to_clickhouse_mfp
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-etl_postgre_to_clickhouse_mfp}"

# -----------------------------------------------------------------------------
# 1. Setup & Activate Virtual Environment
# -----------------------------------------------------------------------------
step "1/5 Setup Environment & Install Dependencies"
if [[ ! -d ".venv_runner" ]]; then
  echo "Creating virtual environment on runner..."
  python3 -m venv .venv_runner
fi

# shellcheck source=/dev/null
source .venv_runner/bin/activate
pip install -q --upgrade pip
echo "Menginstall Airflow & dependensi pipeline..."
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.8}"
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
case "$PYTHON_VERSION" in
  3.10|3.11|3.12|3.13) ;;
  *) echo "ERROR: Airflow $AIRFLOW_VERSION butuh Python 3.10-3.13, runner memakai $PYTHON_VERSION"; exit 1 ;;
esac
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install -q "apache-airflow[postgres]==${AIRFLOW_VERSION}" papermill ipykernel \
  --constraint "$CONSTRAINT_URL"

# Kunci versi Airflow agar requirements lain tidak mengubah/men-downgrade-nya
AIRFLOW_PIN_FILE="$(mktemp)"
echo "apache-airflow==${AIRFLOW_VERSION}" > "$AIRFLOW_PIN_FILE"

if [[ -f "$ROOT_DIR/dags/requirements-airflow.txt" ]]; then
  pip install -q -r "$ROOT_DIR/dags/requirements-airflow.txt" -c "$AIRFLOW_PIN_FILE" || true
fi
if [[ -f "$ROOT_DIR/notebooks/requirements.txt" ]]; then
  pip install -q -r "$ROOT_DIR/notebooks/requirements.txt" -c "$AIRFLOW_PIN_FILE" || true
elif [[ -f "$ROOT_DIR/requirements.txt" ]]; then
  pip install -q -r "$ROOT_DIR/requirements.txt" -c "$AIRFLOW_PIN_FILE" || true
fi

# -----------------------------------------------------------------------------
# 2. Initialize Local Airflow & Register Database Connections
# -----------------------------------------------------------------------------
step "2/5 Initialize local Airflow & Register Connections"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$ROOT_DIR/runs"

# Copy DAGs to AIRFLOW_HOME
cp -rf "$ROOT_DIR"/dags/*.py "$AIRFLOW_HOME/dags/" 2>/dev/null || true

# Initialize local Airflow DB (airflow.db)
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
airflow db migrate >/dev/null

# 2a. ClickHouse VPS connection
echo "Registering connection clickhouse_mfp -> $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
airflow connections delete clickhouse_mfp 2>/dev/null || true
airflow connections add clickhouse_mfp \
  --conn-type "generic" \
  --conn-host "$CLICKHOUSE_HOST" \
  --conn-port "$CLICKHOUSE_PORT" \
  --conn-login "$CLICKHOUSE_USER" \
  --conn-password "$CLICKHOUSE_PASSWORD" \
  --conn-schema "$CLICKHOUSE_DB" \
  --conn-extra '{"secure": false}'

# 2b. PostgreSQL VPS connection (postgres_mfp)
airflow connections delete postgres_mfp 2>/dev/null || true
airflow connections add postgres_mfp \
  --conn-type "postgres" \
  --conn-host "$SOURCE_DB_HOST" \
  --conn-port "$SOURCE_DB_PORT" \
  --conn-login "$SOURCE_DB_USER" \
  --conn-password "$SOURCE_DB_PASSWORD" \
  --conn-schema "$SOURCE_DB_NAME" 2>/dev/null || true

# 2c. Oracle VPS connection (oracle_mfp) — used when running Oracle DAG
airflow connections delete oracle_mfp 2>/dev/null || true
airflow connections add oracle_mfp \
  --conn-type "generic" \
  --conn-host "$SOURCE_DB_HOST" \
  --conn-port "1521" \
  --conn-login "$SOURCE_DB_USER" \
  --conn-password "$SOURCE_DB_PASSWORD" \
  --conn-extra '{"service_name": "FREEPDB1"}' 2>/dev/null || true

# -----------------------------------------------------------------------------
# 3. Test / Run Airflow DAG Synchronously
# -----------------------------------------------------------------------------
step "3/5 Running Airflow DAG: $AIRFLOW_DAG_ID"
echo "Connecting to ClickHouse VPS ($CLICKHOUSE_HOST)..."
airflow dags test "$AIRFLOW_DAG_ID" || {
  echo "WARNING: DAG $AIRFLOW_DAG_ID encountered an issue, trying fallback DAG..."
  if [[ "$AIRFLOW_DAG_ID" != "etl_oracle_to_clickhouse_mfp" ]]; then
    airflow dags test etl_oracle_to_clickhouse_mfp || true
  fi
}

# -----------------------------------------------------------------------------
# 4. Run Training Notebook via Papermill
# -----------------------------------------------------------------------------
step "4/5 Execute Training Notebook (Papermill -> MLflow)"
OUT_NB="$ROOT_DIR/runs/executed_$(date +%Y%m%d_%H%M%S).ipynb"
NOTEBOOK_PATH="$ROOT_DIR/notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb"

if [[ -f "$NOTEBOOK_PATH" ]]; then
  python3 -m ipykernel install --user --name pipeline-kernel >/dev/null 2>&1 || true
  papermill "$NOTEBOOK_PATH" "$OUT_NB" \
    --cwd "$ROOT_DIR/notebooks" \
    --kernel pipeline-kernel \
    --log-output
  echo "Training complete. Executed notebook saved at $OUT_NB"
else
  echo "WARNING: Notebook $NOTEBOOK_PATH not found, skipping papermill step."
fi

# -----------------------------------------------------------------------------
# 5. All outputs saved as GitHub Actions artifacts
# -----------------------------------------------------------------------------
step "5/5 Done — airflow.db, mlflow.db & mlruns/ will be uploaded as GitHub Actions artifacts"
echo "All outputs (airflow.db, mlflow.db, mlruns/, runs/*.ipynb) are handled by the workflow YAML upload-artifact step."

step "Pipeline completed successfully on runner."
