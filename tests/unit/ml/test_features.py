"""Unit tests for spark/ml/src/features.py — pure feature-engineering functions
for the fare-prediction model (Phase 2 of the modeling plan).

Covers: Decimal→float64 casting of climate columns, cyclic hour encoding,
temp_band ordinal encoding, OD-corridor key construction, and the
leakage-safe feature-frame builder (duration ablation flag included).
"""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from spark.ml.src import features


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_frame():
    """Mimics sample_work.parquet's verified schema: climate columns arrive as
    python Decimal objects (pandas dtype 'object'), strings as str."""
    return pd.DataFrame(
        {
            "fare_capped": [12.5, 45.0],
            "fare_amount": [12.5, 61.0],
            "trip_distance": [2.1, 14.15],
            "distance_capped": [2.1, 14.15],
            "trip_duration_min": [11.0, 42.0],
            "passenger_count": pd.array([1, 3], dtype="int32"),
            "temperature": [55.0, 88.2],
            "pickup_hour": pd.array([0, 18], dtype="int32"),
            "pickup_dow": pd.array([2, 6], dtype="int32"),
            "service_type": ["Yellow", "Green"],
            "pickup_borough": ["Manhattan", "Queens"],
            "dropoff_borough": ["Manhattan", "EWR"],
            "pickup_zone": ["Midtown Center", "JFK Airport"],
            "dropoff_zone": ["Union Sq", "Newark Airport"],
            "temp_band": ["Mild", "Hot"],
            "is_airport_trip": pd.array([0, 1], dtype="int32"),
            "ratecodeid": [1, 3],
            "humidity": [Decimal("0.61"), Decimal("0.44")],
            "windSpeed": [Decimal("5.2"), Decimal("11.0")],
            "visibility": [Decimal("9.9"), Decimal("10.0")],
        }
    )


# ---------------------------------------------------------------------------
# cast_decimal_columns
# ---------------------------------------------------------------------------

class TestCastDecimalColumns:
    def test_given_decimal_climate_cols_when_cast_then_dtype_is_float64(self, raw_frame):
        out = features.cast_decimal_columns(raw_frame)
        for col in ("humidity", "windSpeed", "visibility"):
            assert out[col].dtype == np.float64, f"{col} not cast to float64"

    def test_given_decimal_values_when_cast_then_values_preserved(self, raw_frame):
        out = features.cast_decimal_columns(raw_frame)
        assert out["visibility"].tolist() == pytest.approx([9.9, 10.0])
        assert out["humidity"].tolist() == pytest.approx([0.61, 0.44])

    def test_given_none_in_decimal_col_when_cast_then_becomes_nan(self, raw_frame):
        raw_frame.loc[0, "visibility"] = None
        out = features.cast_decimal_columns(raw_frame)
        assert np.isnan(out.loc[0, "visibility"])
        assert out["visibility"].dtype == np.float64

    def test_given_already_float_cols_when_cast_then_untouched(self, raw_frame):
        out = features.cast_decimal_columns(raw_frame)
        assert out["temperature"].dtype == np.float64
        assert out["temperature"].tolist() == raw_frame["temperature"].tolist()

    def test_given_string_object_cols_when_cast_then_left_as_object(self, raw_frame):
        out = features.cast_decimal_columns(raw_frame)
        assert out["pickup_borough"].dtype == object

    def test_when_cast_then_input_frame_not_mutated(self, raw_frame):
        features.cast_decimal_columns(raw_frame)
        assert isinstance(raw_frame.loc[0, "humidity"], Decimal)


# ---------------------------------------------------------------------------
# encode_cyclic_hour
# ---------------------------------------------------------------------------

class TestEncodeCyclicHour:
    def test_given_hour_zero_when_encoded_then_sin_zero_cos_one(self, raw_frame):
        out = features.encode_cyclic_hour(raw_frame)
        assert out.loc[0, "pickup_hour_sin"] == pytest.approx(0.0)
        assert out.loc[0, "pickup_hour_cos"] == pytest.approx(1.0)

    def test_given_hour_six_when_encoded_then_sin_one_cos_zero(self):
        df = pd.DataFrame({"pickup_hour": [6]})
        out = features.encode_cyclic_hour(df)
        assert out.loc[0, "pickup_hour_sin"] == pytest.approx(1.0)
        assert out.loc[0, "pickup_hour_cos"] == pytest.approx(0.0, abs=1e-12)

    def test_given_hour_23_and_0_when_encoded_then_adjacent_on_circle(self):
        df = pd.DataFrame({"pickup_hour": [23, 0, 12]})
        out = features.encode_cyclic_hour(df)
        d_wrap = np.hypot(
            out.loc[0, "pickup_hour_sin"] - out.loc[1, "pickup_hour_sin"],
            out.loc[0, "pickup_hour_cos"] - out.loc[1, "pickup_hour_cos"],
        )
        d_far = np.hypot(
            out.loc[2, "pickup_hour_sin"] - out.loc[1, "pickup_hour_sin"],
            out.loc[2, "pickup_hour_cos"] - out.loc[1, "pickup_hour_cos"],
        )
        assert d_wrap < d_far, "23h→0h should be closer than 12h→0h on the cycle"

    def test_when_encoded_then_raw_pickup_hour_dropped(self, raw_frame):
        out = features.encode_cyclic_hour(raw_frame)
        assert "pickup_hour" not in out.columns


# ---------------------------------------------------------------------------
# encode_temp_band
# ---------------------------------------------------------------------------

class TestEncodeTempBand:
    def test_given_all_bands_when_encoded_then_ordinal_order_preserved(self):
        df = pd.DataFrame({"temp_band": ["Freezing", "Cold", "Mild", "Warm", "Hot"]})
        out = features.encode_temp_band(df)
        assert out["temp_band_ord"].tolist() == [0, 1, 2, 3, 4]

    def test_given_unknown_band_when_encoded_then_raises_value_error(self):
        df = pd.DataFrame({"temp_band": ["Scorching"]})
        with pytest.raises(ValueError, match="Scorching"):
            features.encode_temp_band(df)

    def test_when_encoded_then_raw_temp_band_dropped(self, raw_frame):
        out = features.encode_temp_band(raw_frame)
        assert "temp_band" not in out.columns


# ---------------------------------------------------------------------------
# add_od_corridor
# ---------------------------------------------------------------------------

class TestAddOdCorridor:
    def test_given_zones_when_added_then_key_joins_pickup_and_dropoff(self, raw_frame):
        out = features.add_od_corridor(raw_frame)
        assert out.loc[0, "od_corridor"] == "Midtown Center→Union Sq"

    def test_given_distinct_pairs_when_added_then_keys_distinct(self, raw_frame):
        out = features.add_od_corridor(raw_frame)
        assert out["od_corridor"].nunique() == 2

    def test_given_reversed_pair_when_added_then_direction_matters(self):
        df = pd.DataFrame(
            {"pickup_zone": ["A", "B"], "dropoff_zone": ["B", "A"]}
        )
        out = features.add_od_corridor(df)
        assert out.loc[0, "od_corridor"] != out.loc[1, "od_corridor"]


# ---------------------------------------------------------------------------
# build_features — the leakage-safe assembly
# ---------------------------------------------------------------------------

class TestBuildFeatures:
    def test_given_raw_frame_when_built_then_target_returned_separately(self, raw_frame):
        X, y = features.build_features(raw_frame)
        assert "fare_capped" not in X.columns
        assert y.tolist() == raw_frame["fare_capped"].tolist()

    def test_given_raw_frame_when_built_then_leakage_columns_absent(self, raw_frame):
        X, _ = features.build_features(raw_frame)
        for col in features.EXCLUDED_COLUMNS:
            assert col not in X.columns, f"leakage column '{col}' present in X"

    def test_fare_amount_is_in_excluded_columns(self):
        assert "fare_amount" in features.EXCLUDED_COLUMNS

    def test_given_default_ablation_when_built_then_duration_included(self, raw_frame):
        X, _ = features.build_features(raw_frame)
        assert "trip_duration_min" in X.columns

    def test_given_ablation_off_when_built_then_duration_excluded(self, raw_frame):
        X, _ = features.build_features(raw_frame, include_duration=False)
        assert "trip_duration_min" not in X.columns

    def test_given_raw_frame_when_built_then_climate_cols_are_float64(self, raw_frame):
        X, _ = features.build_features(raw_frame)
        for col in ("humidity", "windSpeed", "visibility"):
            assert X[col].dtype == np.float64

    def test_given_raw_frame_when_built_then_no_object_dtype_except_categoricals(self, raw_frame):
        X, _ = features.build_features(raw_frame)
        object_cols = set(X.select_dtypes(include="object").columns)
        assert object_cols <= set(features.CATEGORICAL_COLUMNS), (
            f"unexpected object columns reach the model: "
            f"{object_cols - set(features.CATEGORICAL_COLUMNS)}"
        )

    def test_given_raw_frame_when_built_then_engineered_columns_present(self, raw_frame):
        X, _ = features.build_features(raw_frame)
        for col in ("pickup_hour_sin", "pickup_hour_cos", "temp_band_ord", "od_corridor"):
            assert col in X.columns
