"""Model registry + sweep runner for the fare-prediction model (Phase 4).

The sweep's contribution is *fairness*, not fitting: every model in the plan's
§5 table is wrapped in Pipeline(preprocessor, model) so preprocessing is fit
inside training folds only, and every one is scored through the single Phase-3
harness on identical folds. That is what makes the leaderboard a comparison
rather than a pile of anecdotes.

Design notes:

- Estimators come from `make_*` **factories**, never module-level constants.
  A shared estimator instance would be fitted by one sweep and silently carried
  into the next.
- `variant` picks the preprocessor: "scaled" for anything distance- or
  gradient-sensitive (linear models, and the ensembles that contain them),
  "tree" for tree-based models where scaling is a no-op.
- xgboost / lightgbm / catboost live only in the dev container
  (see CLAUDE.md § Environments). The registry omits them when they are not
  importable rather than raising, so the host venv gets a smaller-but-valid
  sweep.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from typing import Any

from sklearn.dummy import DummyRegressor
from sklearn.ensemble import (
    BaggingRegressor,
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    RandomForestRegressor,
    StackingRegressor,
    VotingRegressor,
)
from sklearn.linear_model import ElasticNet, Lasso, LinearRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeRegressor

from spark.ml.src.evaluate import RANDOM_STATE, evaluate, leaderboard, make_cv
from spark.ml.src.preprocess import VARIANTS, build_preprocessor

# Families in the plan's §5 table, plus the mean baseline the leaderboard is
# read against.
FAMILIES = (
    "baseline",
    "linear",
    "tree",
    "bagging",
    "boosting",
    "stacking",
    "voting",
)


@dataclass
class ModelSpec:
    """One entry in the sweep registry.

    `variant` names the preprocessor to pair the estimator with; `family` is
    the plan §5 grouping, used to read the leaderboard as a narrative
    (linear underfits → trees overfit → ensembles lead).
    """

    name: str
    estimator: Any
    variant: str
    family: str

    def __post_init__(self) -> None:
        if self.variant not in VARIANTS:
            raise ValueError(
                f"Unknown variant '{self.variant}' — expected one of {VARIANTS}"
            )
        if self.family not in FAMILIES:
            raise ValueError(
                f"Unknown family '{self.family}' — expected one of {FAMILIES}"
            )


def _installed(module: str) -> bool:
    """True if an optional (container-only) library is importable here."""
    return importlib.util.find_spec(module) is not None


# ---------------------------------------------------------------------------
# Estimator factories — a fresh, seeded instance per call
# ---------------------------------------------------------------------------

def make_dummy() -> DummyRegressor:
    """Predict-the-mean floor: any model scoring near this has learned nothing."""
    return DummyRegressor(strategy="mean")


def make_ols() -> LinearRegression:
    return LinearRegression()


def make_ridge() -> Ridge:
    return Ridge(random_state=RANDOM_STATE)


def make_lasso() -> Lasso:
    return Lasso(random_state=RANDOM_STATE)


def make_elasticnet() -> ElasticNet:
    return ElasticNet(random_state=RANDOM_STATE)


def make_decision_tree() -> DecisionTreeRegressor:
    return DecisionTreeRegressor(random_state=RANDOM_STATE)


def make_random_forest() -> RandomForestRegressor:
    return RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=-1)


def make_extra_trees() -> ExtraTreesRegressor:
    return ExtraTreesRegressor(random_state=RANDOM_STATE, n_jobs=-1)


def make_bagging() -> BaggingRegressor:
    return BaggingRegressor(random_state=RANDOM_STATE, n_jobs=-1)


def make_gradient_boosting() -> GradientBoostingRegressor:
    return GradientBoostingRegressor(random_state=RANDOM_STATE)


def make_xgboost():
    """XGBoost regressor — container-only (see module docstring)."""
    from xgboost import XGBRegressor

    return XGBRegressor(random_state=RANDOM_STATE, n_jobs=-1, verbosity=0)


def make_lightgbm():
    """LightGBM regressor — container-only."""
    from lightgbm import LGBMRegressor

    return LGBMRegressor(random_state=RANDOM_STATE, n_jobs=-1, verbose=-1)


def make_catboost():
    """CatBoost regressor — container-only. Seeded via `random_seed`, its own
    parameter name."""
    from catboost import CatBoostRegressor

    return CatBoostRegressor(random_seed=RANDOM_STATE, verbose=0, allow_writing_files=False)


def _make_strongest_booster():
    """XGBoost if present, else sklearn's GradientBoosting — so the ensemble
    bases stay as close to the plan's {RF, XGB, Ridge} as the environment allows."""
    return make_xgboost() if _installed("xgboost") else make_gradient_boosting()


def make_stacking() -> StackingRegressor:
    """Plan §5: bases = {RF, XGB, Ridge}, meta = Ridge."""
    return StackingRegressor(
        estimators=[
            ("rf", make_random_forest()),
            ("boost", _make_strongest_booster()),
            ("ridge", make_ridge()),
        ],
        final_estimator=make_ridge(),
    )


def make_voting() -> VotingRegressor:
    """Plan §5 asks for a vote over the top 2-3. The registry cannot know the
    top before the sweep runs, so it defaults to one member per strong family;
    Phase 5 can re-form it over the actual leaderboard top-3."""
    return VotingRegressor(
        estimators=[
            ("rf", make_random_forest()),
            ("boost", _make_strongest_booster()),
            ("ridge", make_ridge()),
        ]
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def build_model_specs(include_baseline: bool = False) -> list[ModelSpec]:
    """The plan §5 sweep registry.

    Optional boosting libraries are included only where importable. The mean
    baseline is off by default — it is a reading aid for the leaderboard, not
    a candidate model.
    """
    specs: list[ModelSpec] = []

    if include_baseline:
        specs.append(ModelSpec("dummy", make_dummy(), "tree", "baseline"))

    # Linear — scaled, because coefficients are scale-sensitive.
    specs += [
        ModelSpec("ols", make_ols(), "scaled", "linear"),
        ModelSpec("ridge", make_ridge(), "scaled", "linear"),
        ModelSpec("lasso", make_lasso(), "scaled", "linear"),
        ModelSpec("elasticnet", make_elasticnet(), "scaled", "linear"),
    ]

    # Trees and tree ensembles — raw numerics; scaling buys nothing.
    specs += [
        ModelSpec("decision_tree", make_decision_tree(), "tree", "tree"),
        ModelSpec("random_forest", make_random_forest(), "tree", "bagging"),
        ModelSpec("extra_trees", make_extra_trees(), "tree", "bagging"),
        ModelSpec("bagging", make_bagging(), "tree", "bagging"),
        ModelSpec("gradient_boosting", make_gradient_boosting(), "tree", "boosting"),
    ]

    # Container-only boosters.
    if _installed("xgboost"):
        specs.append(ModelSpec("xgboost", make_xgboost(), "tree", "boosting"))
    if _installed("lightgbm"):
        specs.append(ModelSpec("lightgbm", make_lightgbm(), "tree", "boosting"))
    if _installed("catboost"):
        specs.append(ModelSpec("catboost", make_catboost(), "tree", "boosting"))

    # Meta-ensembles hold a Ridge among their bases, so they take the scaled
    # variant; the tree members are indifferent to it.
    specs += [
        ModelSpec("stacking", make_stacking(), "scaled", "stacking"),
        ModelSpec("voting", make_voting(), "scaled", "voting"),
    ]

    return specs


# ---------------------------------------------------------------------------
# Pipeline assembly + sweep execution
# ---------------------------------------------------------------------------

def build_pipeline(spec: ModelSpec, feature_columns) -> Pipeline:
    """Wrap one spec as Pipeline(preprocessor, model).

    Fitting the pipeline fits the preprocessor on the training fold only, which
    is what keeps the target encoding and the scaler leakage-free.
    """
    return Pipeline(
        [
            ("pre", build_preprocessor(spec.variant, feature_columns)),
            ("model", spec.estimator),
        ]
    )


def run_sweep(specs, X, y, cv=None, n_jobs=None):
    """Score every spec through the Phase-3 harness and return the leaderboard.

    All models share one `cv` object so the folds are identical across the
    sweep — the comparison is between models, not between fold draws.
    """
    cv = cv if cv is not None else make_cv()
    rows = [
        evaluate(
            build_pipeline(spec, X.columns),
            X,
            y,
            cv=cv,
            name=spec.name,
            n_jobs=n_jobs,
        )
        for spec in specs
    ]
    return leaderboard(rows)
