"""Shared CV harness + leaderboard for the fare-prediction model sweep
(Phase 3 of the modeling plan).

One evaluate() that every model plugs into — identical KFold folds, identical
metrics — so the Phase-4 sweep is a fair comparison instead of anecdotes.
Models arrive as Pipeline(preprocessor, model) so all preprocessing is fit
inside each training fold (leakage-safe by construction).
"""

import numpy as np
import pandas as pd
from sklearn.metrics import (
    make_scorer,
    mean_absolute_error,
    r2_score,
    root_mean_squared_error,
)
from sklearn.model_selection import KFold, cross_validate, train_test_split

RANDOM_STATE = 42

# Columns the prep stratified its sample on (service_type x temp_band); after
# build_features the band arrives as its ordinal, which is a 1:1 recode.
STRATIFY_COLUMNS = ("service_type", "temp_band_ord")

HOLDOUT_FRACTION = 0.2

# Metric functions — single source for both the CV scorers and any ad-hoc
# reporting (Phase-5 slice metrics reuse these).
mae = mean_absolute_error
rmse = root_mean_squared_error
r2 = r2_score


def compute_metrics(y_true, y_pred) -> dict:
    """All three sweep metrics for one prediction set."""
    return {
        "mae": mae(y_true, y_pred),
        "rmse": rmse(y_true, y_pred),
        "r2": r2(y_true, y_pred),
    }


def make_cv(n_splits: int = 5) -> KFold:
    """The one fold set every model is scored on (shuffled, fixed seed)."""
    return KFold(n_splits=n_splits, shuffle=True, random_state=RANDOM_STATE)


def make_holdout(
    X,
    y,
    test_size: float = HOLDOUT_FRACTION,
    stratify_columns=STRATIFY_COLUMNS,
    random_state: int = RANDOM_STATE,
):
    """Carve the sealed test set off the front of the workflow (plan §4a).

    Returns (X_train, X_test, y_train, y_test). Everything downstream — the
    sweep, CV, hyperparameter search — sees `X_train` only. The test split is
    scored exactly once, in Phase 5, after the champion is chosen on CV
    evidence alone; scoring it repeatedly while tuning silently turns it into
    a validation set and the final number stops being an honest estimate.

    Stratified on `service_type` x `temp_band_ord`, the same key
    00_prep_spark.py sampled with, so the split cannot skew a small stratum
    (Green/Freezing is only ~1.5% of the work sample). Columns that aren't
    present are skipped; with none present the split is plain random, so the
    duration ablation and reduced smoke frames still work.

    Deterministic under `random_state`: the same sample file and seed always
    reproduce the same holdout, which is what keeps it stable across runs
    without persisting a second copy of the data.
    """
    present = [c for c in stratify_columns if c in X.columns]
    strata = None
    if present:
        strata = X[present[0]].astype(str)
        for col in present[1:]:
            strata = strata + "|" + X[col].astype(str)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        shuffle=True,
        stratify=strata,
    )
    return X_train, X_test, y_train, y_test


_SCORING = {
    "mae": make_scorer(mae, greater_is_better=False),
    "rmse": make_scorer(rmse, greater_is_better=False),
    "r2": make_scorer(r2),
}


def evaluate(model, X, y, cv=None, name: str | None = None, n_jobs=None) -> dict:
    """Cross-validate one model and return its leaderboard row.

    `model` is any sklearn estimator — in the sweep, always a
    Pipeline(preprocessor, model). Error scorers come back negated by
    sklearn convention; they are flipped back to positive here.
    """
    cv = cv if cv is not None else make_cv()
    scores = cross_validate(model, X, y, cv=cv, scoring=_SCORING, n_jobs=n_jobs)
    return {
        "model": name if name is not None else type(model).__name__,
        "mae_mean": -scores["test_mae"].mean(),
        "mae_std": scores["test_mae"].std(),
        "rmse_mean": -scores["test_rmse"].mean(),
        "rmse_std": scores["test_rmse"].std(),
        "r2_mean": scores["test_r2"].mean(),
        "r2_std": scores["test_r2"].std(),
        "fit_time_s": scores["fit_time"].mean(),
    }


def leaderboard(rows: list[dict]) -> pd.DataFrame:
    """Assemble evaluate() rows into the running leaderboard, best RMSE first."""
    board = pd.DataFrame(rows)
    if board.empty:
        return board
    return board.sort_values("rmse_mean", ignore_index=True)
