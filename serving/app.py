"""
FastAPI serving untuk prediksi machine failure.

Model TIDAK dibaca dari file lokal: dimuat dari MLflow Model Registry lewat alias
(default `models:/machine_failure_xgb@champion`). Threshold, daftar fitur, dan
type_map ikut dari tag model version, jadi selalu sama dengan saat training.

Jalankan (dev):   uvicorn app:app --reload --port 8000     (dari folder serving/)
Produksi:         pm2 start ecosystem.config.js
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
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]


# ----------------------------------------------------------------- feature engineering
# Harus identik dengan make_features() di notebook.
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
        mlflow.set_tracking_uri(TRACKING_URI)
        client = MlflowClient()
        mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        # muat berdasarkan nomor version (bukan alias) supaya model & tag pasti satu paket
        model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{mv.version}")
        tags = mv.tags
        state = {
            "model": model,
            "version": str(mv.version),
            "features": json.loads(tags["features"]),
            "type_map": json.loads(tags["type_map"]),
            "threshold": float(tags["threshold"]),
            "variant": tags.get("variant"),
            "run_id": mv.run_id,
            "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        with self._lock:
            self._state = state
        log.info("Model dimuat: %s v%s (%s), threshold=%.3f",
                 MODEL_NAME, state["version"], state["variant"], state["threshold"])
        return state

    def get(self) -> dict:
        with self._lock:
            st = self._state
        if st is None:
            raise HTTPException(503, "Model belum termuat dari MLflow. Cek koneksi/registry lalu panggil /reload.")
        return st


store = ModelStore()


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        store.load()
    except Exception as e:  # API tetap hidup; /health menunjukkan model_loaded=false
        log.error("Gagal memuat model dari MLflow: %s", e)
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
    type: Literal["L", "M", "H"] = Field(..., description="Kualitas produk")
    air_temp_k: float = Field(..., ge=250, le=350, description="Suhu udara [K]")
    process_temp_k: float = Field(..., ge=250, le=400, description="Suhu proses [K]")
    rpm: int = Field(..., gt=0, le=10000, description="Kecepatan putar [rpm]")
    torque_nm: float = Field(..., ge=0, le=200, description="Torsi [Nm]")
    tool_wear_min: int = Field(..., ge=0, le=1000, description="Keausan alat [menit]")

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
        return {"status": "ok", "model_loaded": True, "model_version": st["version"]}
    except HTTPException:
        return {"status": "degraded", "model_loaded": False}


@app.get("/model-info")
def model_info():
    st = store.get()
    return {
        "name": MODEL_NAME, "alias": MODEL_ALIAS, "version": st["version"],
        "variant": st["variant"], "threshold": st["threshold"],
        "features": st["features"], "mlflow_run_id": st["run_id"],
        "loaded_at": st["loaded_at"], "tracking_uri": TRACKING_URI,
    }


@app.post("/predict", response_model=Prediction)
def predict(reading: Reading):
    return _predict([reading])[0]


@app.post("/predict/batch", response_model=list[Prediction])
def predict_batch(req: BatchRequest):
    return _predict(req.items)


@app.post("/reload")
def reload_model():
    """Muat ulang model dari MLflow tanpa restart."""
    try:
        st = store.load()
    except Exception as e:
        raise HTTPException(502, f"Gagal memuat model dari MLflow: {e}")
    return {"status": "reloaded", "model_version": st["version"], "threshold": st["threshold"]}
