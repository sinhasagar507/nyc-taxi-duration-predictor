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
            "distance_capped": rng.uniform(0.5, 18.0, N_ROWS),
            "trip_duration_min": rng.uniform(2.0, 55.0, N_ROWS),
            "passenger_count": rng.integers(1, 6, N_ROWS).astype("int32"),
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

class TestSupersededColumnsNeverReachTheModel:
    """Only the transformed half of each raw/derived pair is a model input.

    features.build_features() already excludes the raw halves, so these are
    defence in depth: if a stray raw column ever reaches the preprocessor it
    must be dropped, not silently scaled alongside its own transform.
    """

    def test_capped_distance_is_the_numeric_distance_column(self):
        assert "distance_capped" in preprocess.NUMERIC_COLUMNS
        assert "trip_distance" not in preprocess.NUMERIC_COLUMNS

    def test_temperature_is_not_a_numeric_column(self):
        """temp_band_ord supersedes it."""
        assert "temperature" not in preprocess.NUMERIC_COLUMNS
        assert "temp_band_ord" in preprocess.NUMERIC_COLUMNS

    @pytest.mark.parametrize("variant", ["scaled", "tree"])
    @pytest.mark.parametrize("stray", ["trip_distance", "temperature"])
    def test_stray_raw_column_is_dropped(self, Xy, variant, stray):
        X, y = Xy
        X = X.assign(**{stray: 1e9})
        pre = preprocess.build_preprocessor(variant, X.columns)
        out = pre.fit_transform(X, y)
        assert not any(
            name.endswith(f"__{stray}") for name in pre.get_feature_names_out()
        ), f"{stray} reached the model"
        assert np.abs(out).max() < 1e6, f"{stray} leaked its magnitude into the matrix"


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

class TestDummyTrap:
    """Full one-hot on every categorical makes each block's columns sum to 1,
    so the blocks are linearly dependent on each other. Measured on the real
    sample: rank 25 of 27, cond ~4e15, and the pickup_borough_EWR coefficient
    swinging 8.2 -> 27.1 across CV folds. Predictions survive it (lstsq takes
    the minimum-norm solution) but no coefficient means anything.

    Fixed on the scaled variant only — trees are indifferent to collinearity
    and splitting on a dropped reference level is strictly harder.
    """

    @staticmethod
    def _n_ohe(pre):
        return sum(n.startswith("ohe__") for n in pre.get_feature_names_out())

    def test_scaled_variant_drops_one_level_per_categorical(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("scaled", X.columns)
        pre.fit(X, y)
        levels = sum(X[c].nunique() for c in preprocess.OHE_COLUMNS)
        assert self._n_ohe(pre) == levels - len(preprocess.OHE_COLUMNS)

    def test_tree_variant_keeps_every_level(self, Xy):
        X, y = Xy
        pre = preprocess.build_preprocessor("tree", X.columns)
        pre.fit(X, y)
        levels = sum(X[c].nunique() for c in preprocess.OHE_COLUMNS)
        assert self._n_ohe(pre) == levels

    def test_scaled_design_matrix_is_full_rank(self, Xy):
        X, y = Xy
        out = preprocess.build_preprocessor("scaled", X.columns).fit_transform(X, y)
        rank = np.linalg.matrix_rank(out)
        assert rank == out.shape[1], (
            f"rank {rank} of {out.shape[1]} columns — design matrix is still "
            f"rank-deficient"
        )


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
