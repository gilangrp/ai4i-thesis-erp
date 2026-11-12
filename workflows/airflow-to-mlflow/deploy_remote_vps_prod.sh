#!/usr/bin/env bash
# ==============================================================================
# CI/CD PIPELINE SCRIPT — GITHUB ACTIONS RUNNER -> AIRFLOW ON VPS
# Steps:
# 1. Validate env/secrets (fail fast if any are empty)
# 2. Log in to Airflow VPS (JWT), unpause the DAG, and trigger the DAG run via REST API
# 3. Wait for the DAG run to finish (polling), then write the Airflow link to the GitHub Job Summary
# 4. Run the training notebook via Papermill (read from ClickHouse -> log to MLflow)
#
# Requirements on the VPS: Airflow 3 (api-server, scheduler, dag-processor) is running,
# the DAG code already exists in the VPS dags folder, and the connections
# (clickhouse_mfp, postgres_mfp, oracle_mfp) are already registered in Airflow VPS.
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

step() { echo; echo "==> $*"; }
summary() { [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && echo "$*" >> "$GITHUB_STEP_SUMMARY" || true; }

# -----------------------------------------------------------------------------
# 1. Validate configuration (without credential fallback)
# -----------------------------------------------------------------------------
step "1/4 Validate configuration"
for v in AIRFLOW_BASE_URL AIRFLOW_USERNAME AIRFLOW_PASSWORD AIRFLOW_DAG_ID \
         CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD \
         CLICKHOUSE_DB MLFLOW_TRACKING_URI; do
  if [[ -z "${!v:-}" ]]; then
    echo "ERROR: env '$v' is empty. Set it in GitHub Secrets (Settings > Secrets and variables > Actions)."
    exit 1
  fi
done

AIRFLOW_BASE_URL="${AIRFLOW_BASE_URL%/}"
AIRFLOW_POLL_INTERVAL="${AIRFLOW_POLL_INTERVAL:-10}"
AIRFLOW_WAIT_TIMEOUT="${AIRFLOW_WAIT_TIMEOUT:-1800}"   # seconds
mkdir -p "$ROOT_DIR/runs"

# Alias environment variables for the notebook
export CH_HOST="$CLICKHOUSE_HOST"
export CH_PORT="$CLICKHOUSE_PORT"
export CH_USER="$CLICKHOUSE_USER"
export CH_PASSWORD="$CLICKHOUSE_PASSWORD"
export CH_DATABASE="$CLICKHOUSE_DB"
export CH_TABLE="${CLICKHOUSE_TABLE:-machine_operation}"
export DATA_SOURCE="clickhouse"

# -----------------------------------------------------------------------------
# 2. Log in, unpause, and trigger the DAG on the Airflow VPS
# -----------------------------------------------------------------------------
step "2/4 Trigger DAG '$AIRFLOW_DAG_ID' on $AIRFLOW_BASE_URL"

TOKEN="$(curl -sS --fail-with-body -X POST "$AIRFLOW_BASE_URL/auth/token" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg u "$AIRFLOW_USERNAME" --arg p "$AIRFLOW_PASSWORD" '{username:$u,password:$p}')" \
  | jq -r '.access_token')"
if [[ -z "$TOKEN" || "$TOKEN" == "null" ]]; then
  echo "ERROR: failed to obtain the Airflow token."; exit 1
fi
echo "::add-mask::$TOKEN"

api() {  # api METHOD PATH [curl args...]
  local method="$1" path="$2"; shift 2
  curl -sS --fail-with-body -X "$method" "$AIRFLOW_BASE_URL$path" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "$@"
}

# New DAGs in Airflow are usually paused; manual runs on a paused DAG are not processed by the scheduler
api PATCH "/api/v2/dags/$AIRFLOW_DAG_ID" -d '{"is_paused": false}' >/dev/null

TRIGGER_BODY="$(jq -n \
  --arg note "GitHub Actions run ${GITHUB_RUN_ID:-local}" \
  --arg sha "${GITHUB_SHA:-local}" \
  '{logical_date: null, note: $note, conf: {triggered_by: "github-actions", commit: $sha}}')"

api POST "/api/v2/dags/$AIRFLOW_DAG_ID/dagRuns" -d "$TRIGGER_BODY" \
  | tee "$ROOT_DIR/runs/airflow_trigger.json" >/dev/null

RUN_ID="$(jq -r '.dag_run_id' "$ROOT_DIR/runs/airflow_trigger.json")"
RUN_URL="$AIRFLOW_BASE_URL/dags/$AIRFLOW_DAG_ID/runs/$RUN_ID"
echo "DAG run created: $RUN_ID"
echo "Airflow UI   : $RUN_URL"
summary "### Airflow DAG run"
summary "- DAG: \`$AIRFLOW_DAG_ID\`"
summary "- Run ID: \`$RUN_ID\`"
summary "- Link: $RUN_URL"

# -----------------------------------------------------------------------------
# 3. Wait for the DAG run to finish
# -----------------------------------------------------------------------------
step "3/4 Waiting for DAG run to finish (timeout ${AIRFLOW_WAIT_TIMEOUT}s)"
START_TS="$(date +%s)"
STATE="queued"
while true; do
  STATE="$(api GET "/api/v2/dags/$AIRFLOW_DAG_ID/dagRuns/$RUN_ID" | jq -r '.state')"
  echo "[$(date -u +%H:%M:%S)] state = $STATE"
  case "$STATE" in
    success) break ;;
    failed)  break ;;
  esac
  if (( $(date +%s) - START_TS > AIRFLOW_WAIT_TIMEOUT )); then
    echo "ERROR: timeout while waiting for the DAG run (last state: $STATE)."
    summary "- Status: **timeout** (last state: $STATE)"
    exit 1
  fi
  sleep "$AIRFLOW_POLL_INTERVAL"
done

# Save task status as execution evidence
api GET "/api/v2/dags/$AIRFLOW_DAG_ID/dagRuns/$RUN_ID/taskInstances" \
  > "$ROOT_DIR/runs/airflow_tasks.json" || true
echo "Task summary:"
jq -r '.task_instances[]? | "  - \(.task_id): \(.state) (\(.duration // 0)s)"' \
  "$ROOT_DIR/runs/airflow_tasks.json" || true

if [[ "$STATE" != "success" ]]; then
  echo "ERROR: DAG run ended with state '$STATE'. Check logs at $RUN_URL"
  summary "- Status: **$STATE**"
  exit 1
fi
summary "- Status: **success**"

# Pause before training so ETL output is fully committed/merged in ClickHouse
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-120}"
echo "Airflow is finished. Waiting ${COOLDOWN_SECONDS}s before running the notebook/MLflow..."
sleep "$COOLDOWN_SECONDS"

# -----------------------------------------------------------------------------
# 4. Training notebook via Papermill (Airflow does not need to be installed on the runner)
# -----------------------------------------------------------------------------
step "4/4 Execute Training Notebook (Papermill -> MLflow)"
if [[ ! -d ".venv_runner" ]]; then
  python3 -m venv .venv_runner
fi
# shellcheck source=/dev/null
source .venv_runner/bin/activate
pip install -q --upgrade pip
pip install -q papermill ipykernel

if [[ -f "$ROOT_DIR/notebooks/requirements.txt" ]]; then
  pip install -q -r "$ROOT_DIR/notebooks/requirements.txt" || echo "WARNING: some notebook dependencies failed to install."
elif [[ -f "$ROOT_DIR/requirements.txt" ]]; then
  pip install -q -r "$ROOT_DIR/requirements.txt" || echo "WARNING: some dependencies failed to install."
fi

OUT_NB="$ROOT_DIR/runs/executed_$(date +%Y%m%d_%H%M%S).ipynb"
NOTEBOOK_PATH="$ROOT_DIR/notebooks/SMOTE_ai4i2020_xgboost_machine_failure.ipynb"

if [[ -f "$NOTEBOOK_PATH" ]]; then
  python3 -m ipykernel install --user --name pipeline-kernel >/dev/null 2>&1 || true
  papermill "$NOTEBOOK_PATH" "$OUT_NB" \
    --cwd "$ROOT_DIR/notebooks" \
    --kernel pipeline-kernel \
    --log-output
  echo "Training complete. Executed notebook saved at $OUT_NB"
  summary "### MLflow"
  summary "- Tracking UI: ${MLFLOW_TRACKING_URI}"
else
  echo "WARNING: Notebook $NOTEBOOK_PATH not found, skipping papermill step."
fi

step "Pipeline completed successfully."
