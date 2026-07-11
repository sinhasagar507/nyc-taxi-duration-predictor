"""Unit tests for spark/ml/src/preprocess.py — leakage-safe preprocessing
builders for the fare-prediction model (Phase 2 of the modeling plan).

Covers: the two ColumnTransformer variants (scaled for linear/NN, tree for
tree ensembles), one-hot safety on unseen categories, cross-fitted target
encoding of the high-cardinality od_corridor key, all-numeric output, and
column-list adaptation for the duration ablation.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.compose import ColumnTransformer

from spark.ml.src import preprocess


# ---------------------------------------------------------------------------
# Fixtures — shaped like features.build_features() output (X, y)
# ---------------------------------------------------------------------------

N_ROWS = 40


@pytest.fixture
def Xy():
    rng = np.random.default_rng(42)
    corridors = np.array(["A→B", "B→C", "C→D", "D→A"])[
        rng.integers(0, 4, N_ROWS)
    ]
    # fare correlated with corridor so target encoding has signal
    corridor_effect = {"A→B": 8.0, "B→C": 14.0, "C→D": 22.0, "D→A": 35.0}
    X = pd.DataFrame(
        {
            "trip_distance": rng.uniform(0.5, 18.0, N_ROWS),
            "trip_duration_min": rng.uniform(2.0, 55.0, N_ROWS),
            "passenger_count": rng.integers(1, 6, N_ROWS).astype("int32"),
            "temperature": rng.uniform(10.0, 95.0, N_ROWS),
            "pickup_dow": rng.integers(0, 7, N_ROWS).astype("int32"),
            "service_type": rng.choice(["Yellow", "Green"], N_ROWS),
            "pickup_borough": rng.choice(["Manhattan", "Queens", "Brooklyn"], N_ROWS),
            "dropoff_borough": rng.choice(["Manhattan", "Bronx", "EWR"], N_ROWS),
            "is_airport_trip": rng.integers(0, 2, N_ROWS).astype("int32"),
            "humidity": rng.uniform(0.2, 0.95, N_ROWS),
            "windSpeed": rng.uniform(0.0, 20.0, N_ROWS),
            "visibility": rng.uniform(1.0, 10.0, N_ROWS),
            "pickup_hour_sin": np.sin(2 * np.pi * rng.integers(0, 24, N_ROWS) / 24),
            "pickup_hour_cos": np.cos(2 * np.pi * rng.integers(0, 24, N_ROWS) / 24),
            "temp_band_ord": rng.integers(0, 5, N_ROWS),
            "od_corridor": corridors,
        }
    )
    y = pd.Series(
        [corridor_effect[c] for c in corridors] + rng.normal(0, 1.5, N_ROWS),
        name="fare_capped",
    )
    return X, y


def _cols_with_prefix(preprocessor, prefix):
    """Output-column indices whose feature name carries the given block prefix."""
    names = preprocessor.get_feature_names_out()
    return [i for i, n in enumerate(names) if n.startswith(prefix)]


# ---------------------------------------------------------------------------
# build_preprocessor — construction & validation
# ---------------------------------------------------------------------------

class TestBuildPreprocessor:
    def test_given_scaled_variant_then_returns_column_transformer(self, Xy):
        X, _ = Xy
        pre = preprocess.build_preprocessor("scaled", X.columns)
        assert isinstance(pre, ColumnTransformer)

    def test_given_tree_variant_then_returns_column_transformer(self, Xy):
        X, _ = Xy
        pre = preprocess.build_preprocessor("tree", X.columns)
        assert isinstance(pre, ColumnTransformer)

    def test_given_unknown_variant_then_raises_value_error(self, Xy):
        X, _ = Xy
        with pytest.raises(ValueError, match="bogus"):
            preprocess.build_preprocessor("bogus", X.columns)

    def test_given_ablated_columns_then_duration_not_required(self, Xy):
        X, y = Xy
        X_ablate = X.drop(columns=["trip_duration_min"])
        pre = preprocess.build_preprocessor("tree", X_ablate.columns)
        out = pre.fit_transform(X_ablate, y)  # must not raise
        assert out.shape[0] == N_ROWS


# ---------------------------------------------------------------------------
# Output contract — everything numeric, finite, named
# ---------------------------------------------------------------------------

class TestOutputContract:
    @pytest.mark.parametrize("variant", ["scaled", "tree"])
    def test_fit_transform_returns_dense_float_array(self, Xy, variant):
        X, y = Xy
        pre = preprocess.build_preprocessor(variant, X.columns)
        out = pre.fit_transform(X, y)
        assert isinstance(out, np.ndarray)
        assert out.dtype == np.float64
        assert np.isfinite(out).all()

    @pytest.mark.parametrize("variant", ["scaled", "tree"])
    def test_feature_names_out_available_after_fit(self, Xy, variant):
        X, y = Xy
        pre = preprocess.build_preprocessor(variant, X.columns)
        pre.fit(X, y)
        names = pre.get_feature_names_out()
        assert len(names) == pre.transform(X).shape[1]


# ---------------------------------------------------------------------------
# Scaled vs tree numeric handling
# ---------------------------------------------------------------------------

class TestNumericHandling:
    def test_scaled_variant_standardizes_numerics(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("scaled", X.columns)
        out = pre.fit_transform(X, y)
        idx = _cols_with_prefix(pre, "num__")
        assert idx, "no numeric block in output"
        block = out[:, idx]
        assert np.allclose(block.mean(axis=0), 0.0, atol=1e-9)
        assert np.allclose(block.std(axis=0), 1.0, atol=1e-6)

    def test_tree_variant_passes_numerics_through_unchanged(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("tree", X.columns)
        out = pre.fit_transform(X, y)
        idx = _cols_with_prefix(pre, "num__")
        names = [pre.get_feature_names_out()[i].removeprefix("num__") for i in idx]
        assert np.allclose(out[:, idx], X[names].to_numpy(dtype=np.float64))

    @pytest.mark.parametrize("variant", ["scaled", "tree"])
    def test_binary_flag_stays_zero_one(self, Xy, variant):
        X, y = Xy
        pre = preprocess.build_preprocessor(variant, X.columns)
        out = pre.fit_transform(X, y)
        idx = _cols_with_prefix(pre, "bin__")
        assert idx, "no binary block in output"
        assert set(np.unique(out[:, idx])) <= {0.0, 1.0}


# ---------------------------------------------------------------------------
# Categorical handling — OHE safety, target-encoded corridor
# ---------------------------------------------------------------------------

class TestCategoricalHandling:
    @pytest.mark.parametrize("variant", ["scaled", "tree"])
    def test_unseen_categories_at_transform_do_not_raise(self, Xy, variant):
        X, y = Xy
        pre = preprocess.build_preprocessor(variant, X.columns)
        pre.fit(X, y)
        X_new = X.head(3).copy()
        X_new["pickup_borough"] = "Atlantis"
        X_new["od_corridor"] = "Z→Z"
        out = pre.transform(X_new)
        assert np.isfinite(out).all()

    def test_od_corridor_encoded_to_single_numeric_column(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("tree", X.columns)
        pre.fit(X, y)
        idx = _cols_with_prefix(pre, "te__")
        assert len(idx) == 1, "od_corridor should become exactly one encoded column"

    def test_od_corridor_encoding_reflects_target_signal(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("tree", X.columns)
        pre.fit(X, y)
        out = pre.transform(X)
        te_col = out[:, _cols_with_prefix(pre, "te__")[0]]
        cheap = te_col[X["od_corridor"].to_numpy() == "A→B"].mean()
        dear = te_col[X["od_corridor"].to_numpy() == "D→A"].mean()
        assert dear > cheap, "target encoding lost the corridor→fare ordering"

    def test_target_encoding_is_cross_fitted_in_fit_transform(self, Xy):
        """fit_transform must use out-of-fold encodings (leakage guard):
        its te__ column differs from a plain fit().transform() on the
        same rows, which uses the full-data encoding."""
        X, y = Xy
        pre_a = preprocess.build_preprocessor("tree", X.columns)
        cross_fitted = pre_a.fit_transform(X, y)
        full_fit = pre_a.transform(X)
        te_idx = _cols_with_prefix(pre_a, "te__")[0]
        assert not np.allclose(cross_fitted[:, te_idx], full_fit[:, te_idx]), (
            "fit_transform's target encoding equals full-data transform — "
            "no cross-fitting, target leakage"
        )
