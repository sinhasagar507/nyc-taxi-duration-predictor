"""Unit tests for spark/ml/src/mllib.py — the pure helpers behind the Phase-4b
Spark MLlib GBT baseline (plan §5b).

The fit itself is a script run (01_mllib_baseline.py), like Phase 1; what is
unit-tested here is everything around it that can be tested without a
SparkSession:

  - the column-group builder that feeds StringIndexer/OneHotEncoder vs the raw
    numeric passthrough, including the deliberate od_corridor drop that IS the
    Tier-2 finding of plan §10 rather than an oversight;
  - the adapter turning per-fold RegressionEvaluator metrics into a row that
    evaluate.leaderboard() consumes unchanged, so the Spark result lands in the
    same table as the sklearn sweep instead of a parallel one.

No pyspark import — these are pandas/stdlib-level functions on purpose, so they
run on the host venv as well as in the container.
"""

import numpy as np
import pytest

from spark.ml.src import evaluate as ev
from spark.ml.src import features as ft
from spark.ml.src import mllib
from spark.ml.src import preprocess


# The real 15-feature contract, as recorded in results/sweep_work.json.
WORK_FEATURES = [
    "distance_capped",
    "trip_duration_min",
    "passenger_count",
    "pickup_dow",
    "service_type",
    "pickup_borough",
    "dropoff_borough",
    "is_airport_trip",
    "humidity",
    "windSpeed",
    "visibility",
    "pickup_hour_sin",
    "pickup_hour_cos",
    "temp_band_ord",
    "od_corridor",
]


# ---------------------------------------------------------------------------
# Column groups — what MLlib one-hots vs passes through raw
# ---------------------------------------------------------------------------

class TestColumnGroups:
    def test_od_corridor_is_in_neither_group(self):
        """19,953 one-hot columns is not viable in MLlib (plan §5b). The drop is
        intentional and must be total — appearing in the numeric passthrough
        would be worse than appearing in the categoricals."""
        categorical, numeric, _ = mllib.split_column_groups(WORK_FEATURES)
        assert "od_corridor" not in categorical
        assert "od_corridor" not in numeric

    def test_categorical_group_is_the_low_cardinality_trio(self):
        """Sorted, not in DataFrame order — see
        test_order_is_deterministic_across_calls for why."""
        categorical, _, _ = mllib.split_column_groups(WORK_FEATURES)
        assert categorical == [
            "dropoff_borough",
            "pickup_borough",
            "service_type",
        ]

    def test_numeric_group_is_the_allowlisted_numerics_and_binaries(self):
        _, numeric, _ = mllib.split_column_groups(WORK_FEATURES)
        assert numeric == [
            "distance_capped",
            "humidity",
            "is_airport_trip",
            "passenger_count",
            "pickup_dow",
            "pickup_hour_cos",
            "pickup_hour_sin",
            "temp_band_ord",
            "trip_duration_min",
            "visibility",
            "windSpeed",
        ]

    def test_groups_are_disjoint(self):
        categorical, numeric, _ = mllib.split_column_groups(WORK_FEATURES)
        assert set(categorical).isdisjoint(numeric)

    def test_groups_cover_every_column_except_the_dropped_one(self):
        categorical, numeric, _ = mllib.split_column_groups(WORK_FEATURES)
        covered = set(categorical) | set(numeric)
        assert covered == set(WORK_FEATURES) - {"od_corridor"}

    def test_absent_categorical_is_skipped_not_invented(self):
        """Reduced frames (smoke runs, ablations) must not produce a pipeline
        stage for a column the DataFrame does not have."""
        categorical, numeric, _ = mllib.split_column_groups(
            ["distance_capped", "service_type"]
        )
        assert categorical == ["service_type"]
        assert numeric == ["distance_capped"]

    def test_duration_ablation_frame_has_no_duration_numeric(self):
        columns = [c for c in WORK_FEATURES if c != "trip_duration_min"]
        _, numeric, _ = mllib.split_column_groups(columns)
        assert "trip_duration_min" not in numeric

    def test_order_is_deterministic_across_calls(self):
        """VectorAssembler input order fixes feature-importance indices; a
        set-ordering wobble would silently relabel them between runs."""
        first = mllib.split_column_groups(WORK_FEATURES)
        second = mllib.split_column_groups(list(reversed(WORK_FEATURES)))
        assert first == second

    def test_empty_column_list_gives_three_empty_groups(self):
        assert mllib.split_column_groups([]) == ([], [], [])


# ---------------------------------------------------------------------------
# Unrecognised columns — the allowlist, and saying what it dropped
# ---------------------------------------------------------------------------

class TestUnrecognisedColumns:
    """`numeric` is an allowlist, mirroring preprocess.build_preprocessor's
    `remainder="drop"`, so both stacks discard the same things. But a silent
    drop and a silent include are both ways of not telling you: the dropped
    names come back so the Spark script can print them.
    """

    def test_unknown_column_does_not_become_a_feature(self):
        """The concrete case: a stray pandas index column. Under the old
        catch-all it landed in the numeric passthrough and GBT trained on a
        row number — unlearnable at inference, where there is no row number."""
        _, numeric, _ = mllib.split_column_groups(
            WORK_FEATURES + ["__index_level_0__"]
        )
        assert "__index_level_0__" not in numeric

    def test_unknown_column_is_reported_not_silently_dropped(self):
        *_, unrecognised = mllib.split_column_groups(
            WORK_FEATURES + ["__index_level_0__"]
        )
        assert unrecognised == ["__index_level_0__"]

    def test_leaked_target_column_is_reported_not_trained_on(self):
        """The d83141b failure class: a superseded/leaky column reaching the
        model. sklearn drops it via remainder="drop"; MLlib must too."""
        _, numeric, unrecognised = mllib.split_column_groups(
            WORK_FEATURES + ["fare_amount", "total_amount"]
        )
        assert "fare_amount" not in numeric and "total_amount" not in numeric
        assert unrecognised == ["fare_amount", "total_amount"]

    def test_od_corridor_is_dropped_but_never_called_unrecognised(self):
        """Plan §5b: the corridor exclusion is a documented finding, not an
        anomaly. Lumping the two would bury the finding in noise."""
        *_, unrecognised = mllib.split_column_groups(WORK_FEATURES)
        assert unrecognised == []

    def test_unrecognised_is_sorted_for_stable_reporting(self):
        *_, unrecognised = mllib.split_column_groups(["zzz_col", "aaa_col"])
        assert unrecognised == ["aaa_col", "zzz_col"]

    def test_reduced_frame_reports_nothing_unrecognised(self):
        """Absent is not the same as unknown — the duration ablation and smoke
        frames must stay silent, or the warning becomes noise people ignore."""
        columns = [c for c in WORK_FEATURES if c != "trip_duration_min"]
        *_, unrecognised = mllib.split_column_groups(columns)
        assert unrecognised == []


# ---------------------------------------------------------------------------
# Drift guards — the allowlists are derived, not retyped
# ---------------------------------------------------------------------------

class TestAllowlistsTrackTheirOwners:
    def test_numeric_allowlist_is_sklearn_numerics_plus_binaries(self):
        """GBT does not care that sklearn separates binaries from numerics (it
        splits, it does not scale), but the union must match or the two stacks
        are not training on the same feature set."""
        assert set(mllib.MLLIB_NUMERIC_COLUMNS) == set(
            preprocess.NUMERIC_COLUMNS
        ) | set(preprocess.BINARY_COLUMNS)

    def test_categorical_allowlist_is_the_features_contract_minus_corridor(self):
        assert set(mllib.MLLIB_CATEGORICAL_COLUMNS) == set(
            ft.CATEGORICAL_COLUMNS
        ) - set(mllib.MLLIB_EXCLUDED_COLUMNS)

    def test_allowlists_plus_exclusions_cover_the_whole_feature_contract(self):
        """A new feature added to the prep must be classified deliberately. If
        it is not, it shows up here rather than as a silent drop at run time."""
        classified = (
            set(mllib.MLLIB_NUMERIC_COLUMNS)
            | set(mllib.MLLIB_CATEGORICAL_COLUMNS)
            | set(mllib.MLLIB_EXCLUDED_COLUMNS)
        )
        assert set(WORK_FEATURES) == classified


# ---------------------------------------------------------------------------
# Fold metrics -> leaderboard row
# ---------------------------------------------------------------------------

class TestFoldMetricsToRow:
    def two_folds(self):
        return [
            {"mae": 0.2, "rmse": 1.0, "r2": 0.90},
            {"mae": 0.4, "rmse": 2.0, "r2": 0.98},
        ]

    def test_row_keys_match_the_evaluate_contract(self):
        """A Spark row and an sklearn row have to be the same shape or they
        cannot share a leaderboard."""
        row = mllib.fold_metrics_to_row("mllib_gbt", self.two_folds(), [10.0, 20.0])
        reference = ev.evaluate(
            __import__("sklearn.dummy", fromlist=["DummyRegressor"]).DummyRegressor(),
            np.arange(20).reshape(-1, 1).astype(float),
            np.arange(20).astype(float),
            cv=ev.make_cv(n_splits=2),
            name="reference",
        )
        assert set(row) == set(reference)

    def test_model_name_is_carried_through(self):
        row = mllib.fold_metrics_to_row("mllib_gbt", self.two_folds(), [10.0, 20.0])
        assert row["model"] == "mllib_gbt"

    def test_means_are_hand_computed(self):
        row = mllib.fold_metrics_to_row("m", self.two_folds(), [10.0, 20.0])
        assert row["mae_mean"] == pytest.approx(0.3)
        assert row["rmse_mean"] == pytest.approx(1.5)
        assert row["r2_mean"] == pytest.approx(0.94)

    def test_std_is_population_ddof_zero_like_evaluate(self):
        """evaluate() takes ndarray.std() (ddof=0). A sample std here would make
        Spark rows look tighter or looser than sklearn rows for no real reason."""
        row = mllib.fold_metrics_to_row("m", self.two_folds(), [10.0, 20.0])
        assert row["mae_std"] == pytest.approx(0.1)
        assert row["rmse_std"] == pytest.approx(0.5)
        assert row["r2_std"] == pytest.approx(0.04)

    def test_fit_time_is_the_mean_across_folds(self):
        row = mllib.fold_metrics_to_row("m", self.two_folds(), [10.0, 20.0])
        assert row["fit_time_s"] == pytest.approx(15.0)

    def test_single_fold_has_zero_spread(self):
        row = mllib.fold_metrics_to_row("m", [{"mae": 0.5, "rmse": 1.0, "r2": 0.9}], [3.0])
        assert row["mae_mean"] == pytest.approx(0.5)
        assert row["mae_std"] == pytest.approx(0.0)

    def test_row_sorts_into_a_leaderboard_alongside_sklearn_rows(self):
        spark_row = mllib.fold_metrics_to_row("mllib_gbt", self.two_folds(), [10.0, 20.0])
        sklearn_row = {
            "model": "lightgbm",
            "mae_mean": 0.35,
            "mae_std": 0.001,
            "rmse_mean": 1.05,
            "rmse_std": 0.03,
            "r2_mean": 0.988,
            "r2_std": 0.0007,
            "fit_time_s": 1.0,
        }
        board = ev.leaderboard([spark_row, sklearn_row])
        assert list(board["model"]) == ["lightgbm", "mllib_gbt"]
        assert not board.isna().any().any()

    def test_fold_count_mismatch_raises(self):
        """Silently zipping 3 metrics against 2 timings would drop a fold."""
        with pytest.raises(ValueError):
            mllib.fold_metrics_to_row("m", self.two_folds(), [10.0])

    def test_no_folds_raises(self):
        with pytest.raises(ValueError):
            mllib.fold_metrics_to_row("m", [], [])

    def test_missing_metric_key_raises(self):
        with pytest.raises((KeyError, ValueError)):
            mllib.fold_metrics_to_row("m", [{"mae": 0.2, "rmse": 1.0}], [10.0])
