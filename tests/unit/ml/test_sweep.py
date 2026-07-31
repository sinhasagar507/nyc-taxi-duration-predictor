"""Unit tests for spark/ml/src/sweep.py — the Phase-4 model sweep registry
(plan §5): linear → single tree → bagging → boosting → stacking → voting.

The sweep's job is *not* to fit well; it is to make a fair comparison. So these
tests pin the three things that make it fair:

1. Every plan family is actually represented, with unique names.
2. Each model is wrapped in Pipeline(preprocessor, model) with the correct
   preprocessor variant — scaled for linear/SVM-style models, raw for trees —
   so preprocessing is always fit inside training folds (leakage-safe).
3. Every model is scored through the one Phase-3 harness on identical folds,
   so the leaderboard compares models rather than fold luck.

Optional gradient-boosting libraries (xgboost/lightgbm/catboost) live only in
the dev container; on the host venv the registry must degrade cleanly rather
than raise.
"""

import importlib.util

import numpy as np
import pandas as pd
import pytest
from sklearn.pipeline import Pipeline

from spark.ml.src import evaluate as ev
from spark.ml.src import preprocess as pp
from spark.ml.src import sweep as sw


# Families the plan's §5 table requires; the registry must cover all of them.
PLAN_FAMILIES = {"linear", "tree", "bagging", "boosting", "stacking", "voting"}

# Models that need no third-party install — always present in the registry.
CORE_MODEL_NAMES = {
    "ols",
    "ridge",
    "lasso",
    "elasticnet",
    "decision_tree",
    "random_forest",
    "extra_trees",
    "bagging",
    "gradient_boosting",
    "stacking",
    "voting",
}

# Container-only boosting libraries → registry entries appear conditionally.
OPTIONAL_MODEL_NAMES = {"xgboost", "lightgbm", "catboost"}


def _installed(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


@pytest.fixture
def specs():
    return sw.build_model_specs()


@pytest.fixture
def sweep_data():
    """A small frame shaped like the real feature frame — one numeric driver,
    one low-cardinality categorical, one high-cardinality corridor key — so
    pipelines exercise every preprocessor block."""
    rng = np.random.default_rng(42)
    n = 90
    X = pd.DataFrame(
        {
            "trip_distance": rng.uniform(0.5, 18.0, n),
            "passenger_count": rng.integers(1, 7, n).astype(float),
            "service_type": rng.choice(["Yellow", "Green"], n),
            "od_corridor": rng.choice(["A→B", "B→C", "C→D", "D→A"], n),
        }
    )
    y = pd.Series(4.0 * X["trip_distance"] + rng.normal(0, 0.5, n), name="fare_capped")
    return X, y


# ---------------------------------------------------------------------------
# ModelSpec — the registry entry contract
# ---------------------------------------------------------------------------

class TestModelSpec:
    def test_spec_carries_name_estimator_variant_and_family(self):
        spec = sw.ModelSpec(
            name="ols", estimator=sw.make_ols(), variant="scaled", family="linear"
        )
        assert spec.name == "ols"
        assert spec.variant == "scaled"
        assert spec.family == "linear"
        assert hasattr(spec.estimator, "fit")

    def test_unknown_variant_is_rejected(self):
        with pytest.raises(ValueError, match="variant"):
            sw.ModelSpec(
                name="bad", estimator=sw.make_ols(), variant="quantum", family="linear"
            )

    def test_unknown_family_is_rejected(self):
        with pytest.raises(ValueError, match="family"):
            sw.ModelSpec(
                name="bad", estimator=sw.make_ols(), variant="scaled", family="wizardry"
            )


# ---------------------------------------------------------------------------
# build_model_specs — registry completeness and correctness
# ---------------------------------------------------------------------------

class TestBuildModelSpecs:
    def test_returns_a_non_empty_list_of_specs(self, specs):
        assert isinstance(specs, list)
        assert len(specs) >= len(CORE_MODEL_NAMES)
        assert all(isinstance(s, sw.ModelSpec) for s in specs)

    def test_model_names_are_unique(self, specs):
        names = [s.name for s in specs]
        assert len(names) == len(set(names)), f"duplicate names: {names}"

    def test_every_plan_family_is_represented(self, specs):
        assert PLAN_FAMILIES <= {s.family for s in specs}

    def test_all_core_models_are_present(self, specs):
        assert CORE_MODEL_NAMES <= {s.name for s in specs}

    def test_every_variant_is_one_the_preprocessor_understands(self, specs):
        assert all(s.variant in pp.VARIANTS for s in specs)

    def test_linear_models_use_the_scaled_variant(self, specs):
        linear = [s for s in specs if s.family == "linear"]
        assert linear, "no linear models in the registry"
        assert all(s.variant == "scaled" for s in linear)

    def test_tree_based_models_use_the_raw_tree_variant(self, specs):
        tree_based = [
            s for s in specs if s.family in {"tree", "bagging", "boosting"}
        ]
        assert tree_based, "no tree-based models in the registry"
        assert all(s.variant == "tree" for s in tree_based)

    def test_every_estimator_is_a_fresh_object_per_call(self):
        """Two calls must not hand back the same mutable estimator instance —
        fitting one sweep would otherwise contaminate the next."""
        first = {s.name: s.estimator for s in sw.build_model_specs()}
        second = {s.name: s.estimator for s in sw.build_model_specs()}
        for name in first:
            assert first[name] is not second[name], f"{name} estimator is shared"

    def test_optional_boosting_libs_appear_only_when_installed(self, specs):
        names = {s.name for s in specs}
        for lib in OPTIONAL_MODEL_NAMES:
            assert (lib in names) == _installed(lib), (
                f"{lib} presence in registry does not match installation state"
            )

    def test_seeded_estimators_are_reproducible(self, specs):
        """Anything with a random_state must carry the project seed, or the
        leaderboard is not regenerable (plan §8.2)."""
        for spec in specs:
            params = spec.estimator.get_params()
            if "random_state" in params and params["random_state"] is not None:
                assert params["random_state"] == ev.RANDOM_STATE, (
                    f"{spec.name} uses seed {params['random_state']}"
                )


# ---------------------------------------------------------------------------
# build_pipeline — leakage-safe wrapping
# ---------------------------------------------------------------------------

class TestBuildPipeline:
    def test_returns_pipeline_of_preprocessor_then_model(self, sweep_data):
        X, _ = sweep_data
        spec = sw.ModelSpec(
            name="ols", estimator=sw.make_ols(), variant="scaled", family="linear"
        )
        pipe = sw.build_pipeline(spec, X.columns)
        assert isinstance(pipe, Pipeline)
        assert [name for name, _ in pipe.steps] == ["pre", "model"]

    def test_model_step_is_the_specs_estimator(self, sweep_data):
        X, _ = sweep_data
        estimator = sw.make_ols()
        spec = sw.ModelSpec(
            name="ols", estimator=estimator, variant="scaled", family="linear"
        )
        pipe = sw.build_pipeline(spec, X.columns)
        assert pipe.named_steps["model"] is estimator

    def test_scaled_variant_wires_a_scaler_and_tree_variant_does_not(self, sweep_data):
        X, _ = sweep_data
        scaled = sw.build_pipeline(
            sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear"), X.columns
        )
        tree = sw.build_pipeline(
            sw.ModelSpec("dt", sw.make_decision_tree(), "tree", "tree"), X.columns
        )
        # ColumnTransformer.transformers holds (name, transformer, columns).
        scaled_num = {n: t for n, t, _ in scaled.named_steps["pre"].transformers}["num"]
        tree_num = {n: t for n, t, _ in tree.named_steps["pre"].transformers}["num"]
        assert hasattr(scaled_num, "fit"), "scaled variant should hold a StandardScaler"
        assert tree_num == "passthrough"

    def test_every_registry_pipeline_fits_and_predicts(self, specs, sweep_data):
        X, y = sweep_data
        for spec in specs:
            pipe = sw.build_pipeline(spec, X.columns)
            pipe.fit(X, y)
            preds = pipe.predict(X)
            assert preds.shape == (len(X),), f"{spec.name} predicted wrong shape"
            assert np.isfinite(preds).all(), f"{spec.name} produced non-finite output"


# ---------------------------------------------------------------------------
# run_sweep — the leaderboard, on identical folds
# ---------------------------------------------------------------------------

class TestRunSweep:
    def test_returns_leaderboard_with_one_row_per_spec(self, sweep_data):
        X, y = sweep_data
        specs = [
            sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear"),
            sw.ModelSpec("dt", sw.make_decision_tree(), "tree", "tree"),
        ]
        board = sw.run_sweep(specs, X, y, cv=ev.make_cv(n_splits=3))
        assert isinstance(board, pd.DataFrame)
        assert len(board) == 2
        assert set(board["model"]) == {"ols", "dt"}

    def test_leaderboard_is_sorted_best_rmse_first(self, sweep_data):
        X, y = sweep_data
        specs = [
            sw.ModelSpec("dummy", sw.make_dummy(), "tree", "baseline"),
            sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear"),
        ]
        board = sw.run_sweep(specs, X, y, cv=ev.make_cv(n_splits=3))
        assert board["rmse_mean"].is_monotonic_increasing
        assert board.iloc[0]["model"] == "ols", "linear must beat the mean baseline"

    def test_rows_carry_the_evaluate_contract(self, sweep_data):
        X, y = sweep_data
        specs = [sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear")]
        board = sw.run_sweep(specs, X, y, cv=ev.make_cv(n_splits=3))
        required = {
            "model", "mae_mean", "mae_std", "rmse_mean",
            "rmse_std", "r2_mean", "r2_std", "fit_time_s",
        }
        assert required <= set(board.columns)

    def test_matches_calling_evaluate_directly(self, sweep_data):
        """Proves run_sweep delegates to the Phase-3 harness rather than
        computing its own metrics — same model, same folds, same numbers."""
        X, y = sweep_data
        cv = ev.make_cv(n_splits=3)
        spec = sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear")

        board = sw.run_sweep([spec], X, y, cv=cv)
        direct = ev.evaluate(
            sw.build_pipeline(
                sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear"), X.columns
            ),
            X, y, cv=ev.make_cv(n_splits=3), name="ols",
        )
        assert board.iloc[0]["rmse_mean"] == pytest.approx(direct["rmse_mean"])
        assert board.iloc[0]["mae_mean"] == pytest.approx(direct["mae_mean"])

    def test_two_runs_give_identical_scores(self, sweep_data):
        X, y = sweep_data
        specs = [sw.ModelSpec("dt", sw.make_decision_tree(), "tree", "tree")]
        first = sw.run_sweep(specs, X, y, cv=ev.make_cv(n_splits=3))
        second = sw.run_sweep(
            [sw.ModelSpec("dt", sw.make_decision_tree(), "tree", "tree")],
            X, y, cv=ev.make_cv(n_splits=3),
        )
        assert first.iloc[0]["rmse_mean"] == pytest.approx(second.iloc[0]["rmse_mean"])

    def test_defaults_to_the_shared_cv_when_none_given(self, sweep_data):
        X, y = sweep_data
        specs = [sw.ModelSpec("ols", sw.make_ols(), "scaled", "linear")]
        board = sw.run_sweep(specs, X, y)
        assert len(board) == 1
        assert np.isfinite(board.iloc[0]["rmse_mean"])

    def test_empty_spec_list_gives_empty_leaderboard(self, sweep_data):
        X, y = sweep_data
        board = sw.run_sweep([], X, y, cv=ev.make_cv(n_splits=3))
        assert isinstance(board, pd.DataFrame)
        assert board.empty
