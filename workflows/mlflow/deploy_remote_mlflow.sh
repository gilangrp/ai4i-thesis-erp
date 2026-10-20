#!/usr/bin/env bash
# Menjalankan notebook retraining (Papermill) langsung tanpa melalui Git pull, install dependency, atau Airflow ETL.
# Memulai alur dari Step 5 (Training & Log MLflow) dan lanjut ke Step 6 (Reload FastAPI via PM2).
#
# Penggunaan:
#   bash workflows/run_notebook.sh              # Run notebook + reload FastAPI
#   bash workflows/run_notebook.sh --no-reload  # Run notebook saja tanpa reload FastAPI
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

step() { echo; echo "==> $*"; }

# 1. Konfigurasi environment (kredensial MLflow, ClickHouse, dll ada di .env)
if [[ -f .env ]]; then
  set -a; source .env; set +a
else
  echo "WARNING: .env tidak ditemukan"
fi

VENV="${VENV_DIR:-$APP_DIR/.venv}"
if [[ ! -d "$VENV" ]]; then
  echo "ERROR: Virtualenv tidak ditemukan di $VENV"
  exit 1
fi

# Pastikan kernel mlops-venv terdaftar untuk papermill
"$VENV/bin/python" -m ipykernel install --user --name mlops-venv >/dev/null 2>&1 || true

# Opsi reload FastAPI via pm2 (default: true, bisa dioverride dengan argumen --no-reload)
RELOAD_API="${RELOAD_API:-true}"
if [[ "${1:-}" == "--no-reload" || "${1:-}" == "--skip-reload" ]]; then
  RELOAD_API="false"
fi

# ==============================================================================
# Step 1 : Jalankan notebook
# ==============================================================================
step "1/2 Jalankan notebook (training + log ke MLflow)"
mkdir -p runs
OUT="runs/executed_$(date +%Y%m%d_%H%M%S).ipynb"
echo "Output notebook: $OUT"

"$VENV/bin/papermill" notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb "$OUT" \
    --cwd notebooks --kernel mlops-venv --log-output

# Bersihkan riwayat eksekusi, simpan 10 file terakhir (kompatibel Linux & macOS)
if ls -1t runs/executed_*.ipynb >/dev/null 2>&1; then
  ls -1t runs/executed_*.ipynb | tail -n +11 | while read -r f; do
    [[ -n "$f" ]] && rm -f "$f"
  done
fi

# ==============================================================================
# Step 2 : Reload FastAPI via PM2
# ==============================================================================
if [[ "$RELOAD_API" == "true" ]]; then
  step "2/2 Reload FastAPI via pm2 (model dimuat ulang dari MLflow)"
  if command -v pm2 >/dev/null 2>&1; then
    pm2 startOrReload ecosystem.config.js --update-env
    pm2 save >/dev/null

    # Smoke test: pastikan API hidup dan memuat model dari registry
    API_PORT="${API_PORT:-8000}"
    for i in {1..15}; do
      if curl -fsS "http://127.0.0.1:${API_PORT}/health" 2>/dev/null | grep -q '"model_loaded":true'; then
        echo "API sehat, model termuat."
        exit 0
      fi
      sleep 2
    done
    echo "ERROR: API tidak sehat setelah reload"
    pm2 logs ml-failure-api --lines 40 --nostream || true
    exit 1
  else
    echo "PM2 tidak ditemukan di sistem, melewati reload API."
  fi
else
  echo
  echo "==> Selesai. Eksekusi notebook berhasil (Reload API dilewati)."
fi
