"""
Unit tests for feature engineering logic used in training and serving.
"""
import math
import sys
import unittest.mock as mock
import pandas as pd
import pytest

# Mock xgboost if not installed locally
if "xgboost" not in sys.modules:
    sys.modules["xgboost"] = mock.MagicMock()

from serving.app import make_features


@pytest.fixture
def sample_raw_dataframe():
    """Create sample raw data matching incoming records."""
    return pd.DataFrame({
        "type": ["L", "M", "H"],
        "air_temp_k": [300.0, 298.0, 302.0],
        "process_temp_k": [310.0, 308.0, 312.0],
        "rpm": [1500, 1400, 1600],
        "torque_nm": [40.0, 50.0, 35.0],
        "tool_wear_min": [100, 150, 80],
    })


@pytest.fixture
def type_map():
    return {"L": 0, "M": 1, "H": 2}


def test_make_features_columns(sample_raw_dataframe, type_map):
    """Test that all expected engineered feature columns are present."""
    df_feat = make_features(sample_raw_dataframe, type_map)

    expected_cols = [
        "type_enc",
        "air_temp_k",
        "process_temp_k",
        "rpm",
        "torque_nm",
        "tool_wear_min",
        "temp_diff_k",
        "power_w",
        "wear_torque",
    ]
    for col in expected_cols:
        assert col in df_feat.columns, f"Missing column: {col}"


def test_make_features_calculations(sample_raw_dataframe, type_map):
    """Test engineered mathematical formulas."""
    df_feat = make_features(sample_raw_dataframe, type_map)

    # 1. Type encoding
    assert list(df_feat["type_enc"]) == [0, 1, 2]

    # 2. Temperature difference: process_temp_k - air_temp_k
    assert list(df_feat["temp_diff_k"]) == [10.0, 10.0, 10.0]

    # 3. Power in Watts: torque * rpm * 2 * pi / 60
    expected_power_0 = 40.0 * 1500 * 2 * math.pi / 60
    assert pytest.approx(df_feat["power_w"].iloc[0], rel=1e-4) == expected_power_0

    # 4. Wear * Torque: tool_wear_min * torque_nm
    assert df_feat["wear_torque"].iloc[0] == 100 * 40.0
    assert df_feat["wear_torque"].iloc[1] == 150 * 50.0
