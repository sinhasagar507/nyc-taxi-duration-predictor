"""Unit tests for spark/ml/src/evaluate.py — the shared CV harness that makes
the Phase-4 model sweep a fair comparison (plan §4): every model goes through
the same folds and the same metrics, producing rows for one leaderboard.

Covers: metric functions against hand-computed values, deterministic shared
KFold, the evaluate() row contract, composability with Phase-2 pipelines,
and leaderboard assembly/sorting.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import KFold
from sklearn.pipeline import Pipeline

from spark.ml.src import evaluate as ev


# ---------------------------------------------------------------------------
# Metric functions — hand-computed expectations
# ---------------------------------------------------------------------------

class TestMetricFunctions:
    def test_perfect_prediction_gives_zero_error_full_r2(self):
        y = np.array([1.0, 2.0, 3.0, 4.0])
        assert ev.mae(y, y) == 0.0
        assert ev.rmse(y, y) == 0.0
        assert ev.r2(y, y) == 1.0

    def test_constant_offset_mae_equals_rmse_equals_offset(self):
        y_true = np.array([1.0, 2.0, 3.0])
        y_pred = y_true + 2.0
        assert ev.mae(y_true, y_pred) == pytest.approx(2.0)
        assert ev.rmse(y_true, y_pred) == pytest.approx(2.0)

    def test_rmse_penalizes_outliers_more_than_mae(self):
        y_true = np.zeros(4)
        y_pred = np.array([0.0, 0.0, 0.0, 4.0])  # one big miss
        assert ev.mae(y_true, y_pred) == pytest.approx(1.0)
        assert ev.rmse(y_true, y_pred) == pytest.approx(2.0)
        assert ev.rmse(y_true, y_pred) > ev.mae(y_true, y_pred)

    def test_mean_prediction_gives_zero_r2(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0])
        y_pred = np.full(4, y_true.mean())
        assert ev.r2(y_true, y_pred) == pytest.approx(0.0)

    def test_compute_metrics_returns_all_three(self):
        y = np.array([1.0, 2.0, 3.0])
        m = ev.compute_metrics(y, y + 1.0)
        assert set(m) == {"mae", "rmse", "r2"}
        assert m["mae"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# make_cv — one deterministic fold set for every model
# ---------------------------------------------------------------------------

class TestMakeCv:
    def test_returns_kfold_with_requested_splits(self):
        cv = ev.make_cv(n_splits=7)
        assert isinstance(cv, KFold)
        assert cv.get_n_splits() == 7

    def test_default_is_five_shuffled_seeded_folds(self):
        cv = ev.make_cv()
        assert cv.get_n_splits() == 5
        assert cv.shuffle is True
        assert cv.random_state == 42

    def test_two_instances_yield_identical_folds(self):
        X = np.arange(40).reshape(-1, 1)
        folds_a = [test for _, test in ev.make_cv().split(X)]
        folds_b = [test for _, test in ev.make_cv().split(X)]
        for a, b in zip(folds_a, folds_b):
            assert np.array_equal(a, b), "make_cv folds are not reproducible"


# ---------------------------------------------------------------------------
# evaluate — the row contract
# ---------------------------------------------------------------------------

REQUIRED_ROW_KEYS = {
    "model",
    "mae_mean", "mae_std",
    "rmse_mean", "rmse_std",
    "r2_mean", "r2_std",
    "fit_time_s",
}


@pytest.fixture
def linear_data():
    """y is an exact linear function of X — LinearRegression must ace it."""
    rng = np.random.default_rng(42)
    X = pd.DataFrame({"a": rng.uniform(0, 10, 60), "b": rng.uniform(0, 5, 60)})
    y = pd.Series(3.0 * X["a"] - 2.0 * X["b"] + 7.0, name="target")
    return X, y


class TestEvaluate:
    def test_returns_row_with_required_keys(self, linear_data):
        X, y = linear_data
        row = ev.evaluate(LinearRegression(), X, y, cv=ev.make_cv(n_splits=3))
        assert REQUIRED_ROW_KEYS <= set(row)

    def test_model_name_defaults_to_class_name(self, linear_data):
        X, y = linear_data
        row = ev.evaluate(LinearRegression(), X, y, cv=ev.make_cv(n_splits=3))
        assert row["model"] == "LinearRegression"

    def test_explicit_name_overrides_default(self, linear_data):
        X, y = linear_data
        row = ev.evaluate(
            LinearRegression(), X, y, cv=ev.make_cv(n_splits=3), name="ols_baseline"
        )
        assert row["model"] == "ols_baseline"

    def test_exact_linear_relation_scores_near_perfect(self, linear_data):
        X, y = linear_data
        row = ev.evaluate(LinearRegression(), X, y, cv=ev.make_cv(n_splits=3))
        assert row["mae_mean"] == pytest.approx(0.0, abs=1e-8)
        assert row["r2_mean"] == pytest.approx(1.0, abs=1e-8)

    def test_dummy_mean_regressor_scores_near_zero_r2(self, linear_data):
        X, y = linear_data
        row = ev.evaluate(DummyRegressor(), X, y, cv=ev.make_cv(n_splits=3))
        assert row["r2_mean"] < 0.1
        assert row["rmse_mean"] > 0.0

    def test_composes_with_phase2_pipeline(self):
        """The harness must accept Pipeline(preprocessor, model) unchanged —
        that is how every Phase-4 sweep entry will arrive."""
        from spark.ml.src.preprocess import build_preprocessor

        rng = np.random.default_rng(42)
        n = 60
        X = pd.DataFrame(
            {
                "trip_distance": rng.uniform(0.5, 18.0, n),
                "service_type": rng.choice(["Yellow", "Green"], n),
                "od_corridor": rng.choice(["A→B", "B→C", "C→D"], n),
            }
        )
        y = pd.Series(4.0 * X["trip_distance"] + rng.normal(0, 0.5, n))
        pipe = Pipeline(
            [
                ("pre", build_preprocessor("scaled", X.columns)),
                ("model", LinearRegression()),
            ]
        )
        row = ev.evaluate(pipe, X, y, cv=ev.make_cv(n_splits=3), name="ridge_pipe")
        assert row["r2_mean"] > 0.9


# ---------------------------------------------------------------------------
# leaderboard — assembly and ranking
# ---------------------------------------------------------------------------

class TestLeaderboard:
    def test_rows_become_dataframe_sorted_by_rmse(self):
        rows = [
            {"model": "worse", "mae_mean": 3.0, "mae_std": 0.1,
             "rmse_mean": 5.0, "rmse_std": 0.2, "r2_mean": 0.5, "r2_std": 0.02,
             "fit_time_s": 1.0},
            {"model": "better", "mae_mean": 1.0, "mae_std": 0.1,
             "rmse_mean": 2.0, "rmse_std": 0.2, "r2_mean": 0.9, "r2_std": 0.01,
             "fit_time_s": 2.0},
        ]
        board = ev.leaderboard(rows)
        assert isinstance(board, pd.DataFrame)
        assert board.iloc[0]["model"] == "better"
        assert board["rmse_mean"].is_monotonic_increasing

    def test_empty_rows_give_empty_frame(self):
        board = ev.leaderboard([])
        assert isinstance(board, pd.DataFrame)
        assert board.empty
