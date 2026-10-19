#!/usr/bin/env bash
# Dijalankan DI SERVER lewat SSH oleh GitHub Actions.
# Urutan: update kode -> install dependency -> sync DAG -> trigger DAG (API)
#         -> jalankan notebook (train + log ke MLflow) -> reload FastAPI (pm2)
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

step() { echo; echo "==> $*"; }

# Konfigurasi server (kredensial Airflow, MLflow, ClickHouse) ada di .env, bukan di GitHub
if [[ -f .env ]]; then set -a; source .env; set +a; else echo "WARNING: .env tidak ditemukan"; fi

GIT_REF="${GIT_REF:-main}"
export GIT_SHA="${GIT_SHA:-$(git rev-parse HEAD)}"
VENV="${VENV_DIR:-$APP_DIR/.venv}"

step "1/6 Update kode ($GIT_REF @ ${GIT_SHA:0:7})"
git fetch --prune origin
git checkout -q "$GIT_REF"
git reset --hard "$GIT_SHA"

step "2/6 Install dependency"
[[ -d "$VENV" ]] || python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r requirements.txt
"$VENV/bin/python" -m ipykernel install --user --name mlops-venv >/dev/null

step "3/6 Sync DAG ke Airflow ($AIRFLOW_DAGS_DIR)"
: "${AIRFLOW_DAGS_DIR:?isi AIRFLOW_DAGS_DIR di .env}"
cp -f dags/*.py "$AIRFLOW_DAGS_DIR/"

step "4/6 Trigger DAG via Airflow API"
"$VENV/bin/python" scripts/trigger_airflow_dag.py

step "5/6 Jalankan notebook (training + log ke MLflow)"
mkdir -p runs
OUT="runs/executed_$(date +%Y%m%d_%H%M%S).ipynb"
"$VENV/bin/papermill" notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb "$OUT" \
    --cwd notebooks --kernel mlops-venv --log-output
ls -1t runs/executed_*.ipynb | tail -n +11 | xargs -r rm -f   # simpan 10 terakhir

step "6/6 Reload FastAPI via pm2 (model dimuat ulang dari MLflow)"
pm2 startOrReload ecosystem.config.js --update-env
pm2 save >/dev/null

# Smoke test: pastikan API hidup dan memuat model dari registry
API_PORT="${API_PORT:-8000}"
for i in {1..15}; do
  if curl -fsS "http://127.0.0.1:${API_PORT}/health" | grep -q '"model_loaded":true'; then
    echo "API sehat, model termuat."; exit 0
  fi
  sleep 2
done
echo "ERROR: API tidak sehat setelah reload"; pm2 logs ml-failure-api --lines 40 --nostream || true
exit 1
