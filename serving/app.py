"""
FastAPI serving application for machine failure prediction.

By default, the model is loaded from the MLflow Model Registry using the
`models:/machine_failure_xgb@champion` alias. Set `IS_MODEL_FROM_MLFLOW=0` to load
the local model from `serving/model/model.ubj` and its accompanying metadata.

Development:  uvicorn app:app --reload --port 8000 (from the serving/ directory)
Production:   pm2 start ecosystem.config.js
"""
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import mlflow
import mlflow.xgboost
import pandas as pd
import xgboost as xgb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, Field

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

log = logging.getLogger("uvicorn.error")
TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "machine_failure_xgb")
MODEL_ALIAS = os.getenv("MLFLOW_MODEL_ALIAS", "champion")
IS_MODEL_FROM_MLFLOW_VALUE = os.getenv("IS_MODEL_FROM_MLFLOW", "1")
if IS_MODEL_FROM_MLFLOW_VALUE not in {"0", "1"}:
    raise ValueError("IS_MODEL_FROM_MLFLOW must be either '1' or '0'.")
IS_MODEL_FROM_MLFLOW = IS_MODEL_FROM_MLFLOW_VALUE == "1"
LOCAL_MODEL_DIR = Path(__file__).resolve().parent / "model"
LOCAL_MODEL_PATH = LOCAL_MODEL_DIR / "model.ubj"
LOCAL_MODEL_METADATA_PATH = LOCAL_MODEL_DIR / "model_meta.json"
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]


# ----------------------------------------------------------------- feature engineering
# Keep this implementation in sync with make_features() in the notebook.
def make_features(d: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    out = pd.DataFrame(index=d.index)
    out["type_enc"] = d["type"].map(type_map)
    out["air_temp_k"] = d["air_temp_k"]
    out["process_temp_k"] = d["process_temp_k"]
    out["rpm"] = d["rpm"]
    out["torque_nm"] = d["torque_nm"]
    out["tool_wear_min"] = d["tool_wear_min"]
    out["temp_diff_k"] = d["process_temp_k"] - d["air_temp_k"]
    out["power_w"] = d["torque_nm"] * d["rpm"] * 2 * 3.141592653589793 / 60
    out["wear_torque"] = d["tool_wear_min"] * d["torque_nm"]
    return out


# ----------------------------------------------------------------- model store
class ModelStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict | None = None

    def load(self) -> dict:
        if IS_MODEL_FROM_MLFLOW:
            mlflow.set_tracking_uri(TRACKING_URI)
            client = MlflowClient()
            mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
            # Load by version number so the model and its tags stay paired.
            model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{mv.version}")
            tags = mv.tags
            version = str(mv.version)
            run_id = mv.run_id
            source = "mlflow"
        else:
            model = xgb.XGBClassifier()
            model.load_model(str(LOCAL_MODEL_PATH))
            tags = json.loads(LOCAL_MODEL_METADATA_PATH.read_text(encoding="utf-8"))
            version = "local"
            run_id = tags.get("run_id")
            source = "local"

        features = tags["features"]
        type_map = tags["type_map"]
        if isinstance(features, str):
            features = json.loads(features)
        if isinstance(type_map, str):
            type_map = json.loads(type_map)

        state = {
            "model": model,
            "version": version,
            "features": features,
            "type_map": type_map,
            "threshold": float(tags["threshold"]),
            "variant": tags.get("variant"),
            "run_id": run_id,
            "source": source,
            "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        with self._lock:
            self._state = state
        log.info("Loaded model from %s: %s v%s (%s), threshold=%.3f",
                 source, MODEL_NAME, state["version"], state["variant"], state["threshold"])
        return state

    def get(self) -> dict:
        with self._lock:
            st = self._state
        if st is None:
            raise HTTPException(503, "Model is not loaded. Check startup logs and model source configuration.")
        return st


store = ModelStore()


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        store.load()
    except Exception as e:  # Keep the API running; /health reports model_loaded=false.
        log.error("Failed to load model: %s", e)
    yield


app = FastAPI(title="Machine Failure API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------- schema
class Reading(BaseModel):
    type: Literal["L", "M", "H"] = Field(..., description="Product quality")
    air_temp_k: float = Field(..., ge=250, le=350, description="Air temperature [K]")
    process_temp_k: float = Field(..., ge=250, le=400, description="Process temperature [K]")
    rpm: int = Field(..., gt=0, le=10000, description="Rotational speed [rpm]")
    torque_nm: float = Field(..., ge=0, le=200, description="Torque [Nm]")
    tool_wear_min: int = Field(..., ge=0, le=1000, description="Tool wear [minutes]")

    model_config = {"json_schema_extra": {"examples": [{
        "type": "L", "air_temp_k": 300.5, "process_temp_k": 310.9,
        "rpm": 1500, "torque_nm": 55.0, "tool_wear_min": 215}]}}


class Prediction(BaseModel):
    prediction: int
    label: Literal["FAILURE", "NORMAL"]
    probability: float
    threshold: float
    model_version: str


class BatchRequest(BaseModel):
    items: list[Reading] = Field(..., min_length=1, max_length=1000)


def _predict(rows: list[Reading]) -> list[Prediction]:
    st = store.get()
    df = pd.DataFrame([r.model_dump() for r in rows])
    X = make_features(df, st["type_map"])[st["features"]]
    proba = st["model"].predict_proba(X)[:, 1]
    return [
        Prediction(prediction=int(p >= st["threshold"]),
                   label="FAILURE" if p >= st["threshold"] else "NORMAL",
                   probability=round(float(p), 6), threshold=st["threshold"],
                   model_version=st["version"])
        for p in proba
    ]


# ----------------------------------------------------------------- endpoints
@app.get("/main")
def main_page():
    return FileResponse(Path(__file__).resolve().parent / "index.html")


@app.get("/banner-image.jpeg")
def banner_image():
    return FileResponse(Path(__file__).resolve().parent / "banner-image.jpeg")


@app.get("/health")
def health():
    try:
        st = store.get()
        return {
            "status": "ok", "model_loaded": True,
            "model_source": st["source"], "model_version": st["version"],
        }
    except HTTPException:
        return {"status": "degraded", "model_loaded": False}


@app.get("/model-info")
def model_info():
    st = store.get()
    return {
        "name": MODEL_NAME, "alias": MODEL_ALIAS if IS_MODEL_FROM_MLFLOW else None,
        "version": st["version"], "source": st["source"],
        "variant": st["variant"], "threshold": st["threshold"],
        "features": st["features"], "mlflow_run_id": st["run_id"],
        "loaded_at": st["loaded_at"],
        "tracking_uri": TRACKING_URI if IS_MODEL_FROM_MLFLOW else None,
    }


@app.post("/predict", response_model=Prediction)
def predict(reading: Reading):
    return _predict([reading])[0]


@app.post("/predict/batch", response_model=list[Prediction])
def predict_batch(req: BatchRequest):
    return _predict(req.items)


@app.post("/reload")
def reload_model():
    """Reload the model without restarting the API."""
    try:
        st = store.load()
    except Exception as e:
        raise HTTPException(502, f"Failed to load model: {e}")
    return {"status": "reloaded", "model_version": st["version"], "threshold": st["threshold"]}
