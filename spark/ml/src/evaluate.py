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
from sklearn.model_selection import KFold, cross_validate

RANDOM_STATE = 42

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
