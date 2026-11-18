"""
Unit and integration tests for FastAPI Serving Application.
Validates Pydantic schema validation, endpoints, and inference handling.
"""
import sys
import unittest.mock as mock
import numpy as np
import pytest
from pydantic import ValidationError

# Mock xgboost if not installed locally
if "xgboost" not in sys.modules:
    sys.modules["xgboost"] = mock.MagicMock()

from fastapi.testclient import TestClient
from serving.app import app, store, Reading, BatchRequest


@pytest.fixture
def test_client():
    """Create TestClient and ensure model predict_proba returns expected array."""
    with TestClient(app) as client:
        # If store didn't load (or loaded a mock), provide valid state
        try:
            st = store.get()
        except Exception:
            mock_model = mock.MagicMock()
            st = {
                "model": mock_model,
                "version": "1",
                "features": [
                    "type_enc", "air_temp_k", "process_temp_k", "rpm",
                    "torque_nm", "tool_wear_min", "temp_diff_k", "power_w", "wear_torque"
                ],
                "type_map": {"L": 0, "M": 1, "H": 2},
                "threshold": 0.5,
                "variant": "xgb_test",
                "run_id": "test_run_123",
                "source": "mock",
                "loaded_at": "2026-11-18T00:00:00Z",
            }
            with store._lock:
                store._state = st

        # Ensure predict_proba is callable returning numpy probabilities
        if isinstance(st["model"], mock.MagicMock):
            st["model"].predict_proba.return_value = np.array([[0.8, 0.2]])

        yield client


def test_reading_schema_valid():
    """Verify valid reading model instantiates correctly."""
    r = Reading(
        type="L",
        air_temp_k=300.0,
        process_temp_k=310.0,
        rpm=1500,
        torque_nm=40.0,
        tool_wear_min=100
    )
    assert r.type == "L"
    assert r.rpm == 1500


def test_reading_schema_invalid_type():
    """Verify invalid product type fails validation."""
    with pytest.raises(ValidationError):
        Reading(
            type="INVALID",
            air_temp_k=300.0,
            process_temp_k=310.0,
            rpm=1500,
            torque_nm=40.0,
            tool_wear_min=100
        )


def test_reading_schema_out_of_bounds():
    """Verify physical constraints validation (e.g. negative RPM or extreme temps)."""
    with pytest.raises(ValidationError):
        Reading(
            type="L",
            air_temp_k=100.0,  # Below 250K
            process_temp_k=310.0,
            rpm=1500,
            torque_nm=40.0,
            tool_wear_min=100
        )

    with pytest.raises(ValidationError):
        Reading(
            type="L",
            air_temp_k=300.0,
            process_temp_k=310.0,
            rpm=-50,  # Invalid RPM <= 0
            torque_nm=40.0,
            tool_wear_min=100
        )


def test_api_health_endpoint(test_client):
    """Verify /health endpoint returns HTTP 200 and healthy status."""
    response = test_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["model_loaded"] is True


def test_api_predict_endpoint(test_client):
    """Verify /predict endpoint returns proper prediction response structure."""
    st = store.get()
    if isinstance(st["model"], mock.MagicMock):
        st["model"].predict_proba.return_value = np.array([[0.8, 0.2]])

    payload = {
        "type": "L",
        "air_temp_k": 300.0,
        "process_temp_k": 310.0,
        "rpm": 1500,
        "torque_nm": 40.0,
        "tool_wear_min": 100
    }
    response = test_client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "prediction" in data
    assert "label" in data
    assert "probability" in data
    assert "threshold" in data
    assert "model_version" in data
    assert data["label"] in ["NORMAL", "FAILURE"]


def test_api_predict_batch_endpoint(test_client):
    """Verify /predict/batch endpoint processes list of records."""
    st = store.get()
    if isinstance(st["model"], mock.MagicMock):
        st["model"].predict_proba.return_value = np.array([[0.8, 0.2], [0.1, 0.9]])

    payload = {
        "items": [
            {
                "type": "L",
                "air_temp_k": 300.0,
                "process_temp_k": 310.0,
                "rpm": 1500,
                "torque_nm": 40.0,
                "tool_wear_min": 100
            },
            {
                "type": "H",
                "air_temp_k": 298.0,
                "process_temp_k": 308.0,
                "rpm": 1400,
                "torque_nm": 45.0,
                "tool_wear_min": 120
            }
        ]
    }
    response = test_client.post("/predict/batch", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["label"] == "NORMAL"
    assert data[1]["label"] == "FAILURE"
