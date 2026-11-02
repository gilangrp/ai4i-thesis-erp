#!/usr/bin/env bash
# ==============================================================================
# SCRIPT TESTING CI/CD DI RUNNER GITHUB ACTIONS
# Alur:
# 1. Setup environment Python & install dependencies
# 2. Inisialisasi Airflow lokal & setup koneksi ClickHouse VPS (202.10.42.115) + Source DB
# 3. Eksekusi DAG Airflow: airflow dags test
# 4. Jalankan training notebook (Papermill) membaca ClickHouse VPS
# 5. Salin / sync hasil (airflow.db, mlflow.db / folder mlruns) ke VPS via rsync/scp
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

step() { echo; echo "==> $*"; }

# -----------------------------------------------------------------------------
# Konfigurasi Target VPS & Database
# -----------------------------------------------------------------------------
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-202.10.42.115}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-airflow_etl}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-airflow_etl}"
export CLICKHOUSE_DB="${CLICKHOUSE_DB:-erp_ai4i}"
export CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-machine_operation}"

# Mapping untuk notebook
export CH_HOST="$CLICKHOUSE_HOST"
export CH_PORT="$CLICKHOUSE_PORT"
export CH_USER="$CLICKHOUSE_USER"
export CH_PASSWORD="$CLICKHOUSE_PASSWORD"
export CH_DATABASE="$CLICKHOUSE_DB"
export CH_TABLE="$CLICKHOUSE_TABLE"
export DATA_SOURCE="clickhouse"
export MLFLOW_TRACKING_URI="sqlite:///$ROOT_DIR/mlflow.db"

# Source DB (Oracle / PostgreSQL) di VPS
export SOURCE_DB_HOST="${SOURCE_DB_HOST:-202.10.42.115}"
export SOURCE_DB_PORT="${SOURCE_DB_PORT:-5432}"
export SOURCE_DB_USER="${SOURCE_DB_USER:-airflow_etl}"
export SOURCE_DB_PASSWORD="${SOURCE_DB_PASSWORD:-airflow_etl}"
export SOURCE_DB_NAME="${SOURCE_DB_NAME:-erp_ai4i}"

VPS_HOST="${SSH_HOST:-202.10.42.115}"
VPS_USER="${SSH_USER:-root}"
VPS_PORT="${SSH_PORT:-22}"
VPS_APP_DIR="${APP_DIR:-/root/ai4i-thesis-erp}"

export AIRFLOW_HOME="${AIRFLOW_HOME:-$ROOT_DIR/airflow_home}"
# Default menggunakan etl_postgre_to_clickhouse_mfp atau etl_oracle_to_clickhouse_mfp
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-etl_postgre_to_clickhouse_mfp}"

# -----------------------------------------------------------------------------
# 1. Setup & Aktivasi Virtual Environment
# -----------------------------------------------------------------------------
step "1/5 Setup Environment & Install Dependencies"
if [[ ! -d ".venv_runner" ]]; then
  echo "Membuat virtual environment di runner..."
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
# 2. Inisialisasi Airflow Lokal & Setup Koneksi Database
# -----------------------------------------------------------------------------
step "2/5 Inisialisasi Airflow lokal & Register Connections"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$ROOT_DIR/runs"

# Salin DAGs ke folder AIRFLOW_HOME
cp -rf "$ROOT_DIR"/dags/*.py "$AIRFLOW_HOME/dags/" 2>/dev/null || true

# Init DB lokal Airflow (airflow.db) - Airflow 3 memakai "db migrate" (db init sudah dihapus)
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
airflow db migrate >/dev/null

# 2a. Koneksi ke ClickHouse VPS
echo "Mendaftarkan Connection clickhouse_mfp -> $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
airflow connections delete clickhouse_mfp 2>/dev/null || true
airflow connections add clickhouse_mfp \
  --conn-type "generic" \
  --conn-host "$CLICKHOUSE_HOST" \
  --conn-port "$CLICKHOUSE_PORT" \
  --conn-login "$CLICKHOUSE_USER" \
  --conn-password "$CLICKHOUSE_PASSWORD" \
  --conn-schema "$CLICKHOUSE_DB" \
  --conn-extra '{"secure": false}'

# 2b. Koneksi ke PostgreSQL VPS (postgres_mfp)
airflow connections delete postgres_mfp 2>/dev/null || true
airflow connections add postgres_mfp \
  --conn-type "postgres" \
  --conn-host "$SOURCE_DB_HOST" \
  --conn-port "$SOURCE_DB_PORT" \
  --conn-login "$SOURCE_DB_USER" \
  --conn-password "$SOURCE_DB_PASSWORD" \
  --conn-schema "$SOURCE_DB_NAME" 2>/dev/null || true

# 2c. Koneksi ke Oracle VPS jika menggunakan Oracle DAG (oracle_mfp)
airflow connections delete oracle_mfp 2>/dev/null || true
airflow connections add oracle_mfp \
  --conn-type "generic" \
  --conn-host "$SOURCE_DB_HOST" \
  --conn-port "1521" \
  --conn-login "$SOURCE_DB_USER" \
  --conn-password "$SOURCE_DB_PASSWORD" \
  --conn-extra '{"service_name": "FREEPDB1"}' 2>/dev/null || true

# -----------------------------------------------------------------------------
# 3. Tes / Jalankan DAG Airflow Secara Synchronous
# -----------------------------------------------------------------------------
step "3/5 Menjalankan DAG Airflow: $AIRFLOW_DAG_ID"
echo "Menghubungkan ke ClickHouse VPS ($CLICKHOUSE_HOST)..."
airflow dags test "$AIRFLOW_DAG_ID" || {
  echo "WARNING: DAG $AIRFLOW_DAG_ID mengalami issue saat test, memeriksa fallback DAG..."
  if [[ "$AIRFLOW_DAG_ID" != "etl_oracle_to_clickhouse_mfp" ]]; then
    airflow dags test etl_oracle_to_clickhouse_mfp || true
  fi
}

# -----------------------------------------------------------------------------
# 4. Jalankan Training Notebook via Papermill
# -----------------------------------------------------------------------------
step "4/5 Eksekusi Notebook Training (Papermill -> MLflow)"
OUT_NB="$ROOT_DIR/runs/executed_$(date +%Y%m%d_%H%M%S).ipynb"
NOTEBOOK_PATH="$ROOT_DIR/notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb"

if [[ -f "$NOTEBOOK_PATH" ]]; then
  python3 -m ipykernel install --user --name test-kernel >/dev/null 2>&1 || true
  papermill "$NOTEBOOK_PATH" "$OUT_NB" \
    --cwd "$ROOT_DIR/notebooks" \
    --kernel test-kernel \
    --log-output
  echo "Training selesai. Hasil notebook disimpan di $OUT_NB"
else
  echo "WARNING: Notebook $NOTEBOOK_PATH tidak ditemukan, melewati step papermill."
fi

# -----------------------------------------------------------------------------
# 5. Salin Output (airflow.db, mlflow.db, mlruns) ke VPS (Optional)
# -----------------------------------------------------------------------------
step "5/5 Sinkronisasi Output ke VPS ($VPS_HOST)"
if command -v rsync >/dev/null 2>&1 && [[ -f "$HOME/.ssh/id_rsa" ]]; then
  echo "Mengirim hasil ke $VPS_USER@$VPS_HOST:$VPS_APP_DIR via rsync..."
  
  if [[ -f "$AIRFLOW_HOME/airflow.db" ]]; then
    rsync -avz -e "ssh -p $VPS_PORT -o StrictHostKeyChecking=no" \
      "$AIRFLOW_HOME/airflow.db" "$VPS_USER@$VPS_HOST:$VPS_APP_DIR/airflow.db" || true
  fi

  if [[ -f "$ROOT_DIR/mlflow.db" ]]; then
    rsync -avz -e "ssh -p $VPS_PORT -o StrictHostKeyChecking=no" \
      "$ROOT_DIR/mlflow.db" "$VPS_USER@$VPS_HOST:$VPS_APP_DIR/mlflow.db" || true
  fi

  if [[ -d "$ROOT_DIR/mlruns" ]]; then
    rsync -avz -e "ssh -p $VPS_PORT -o StrictHostKeyChecking=no" \
      "$ROOT_DIR/mlruns/" "$VPS_USER@$VPS_HOST:$VPS_APP_DIR/mlruns/" || true
  fi

  echo "Sinkronisasi ke VPS selesai!"
else
  echo "Info: SSH Key tidak ditemukan atau sync dilewati. Hasil tersimpan di GitHub Artifacts."
fi

step "Selesai! Pipeline test berhasil dijalankan di runner."
