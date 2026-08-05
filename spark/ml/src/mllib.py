"""Pure helpers for the Phase-4b Spark MLlib GBT baseline (plan §5b).

Deliberately free of any `pyspark` import. The fit itself is a script run
(01_mllib_baseline.py) like Phase 1, but the decisions *around* the fit — which
columns get one-hot encoded, which pass through raw, and how a Spark result
becomes a leaderboard row — are ordinary data transformations, so they live
here and are unit-tested on the host venv without a SparkSession.

Two contracts matter:

  - `od_corridor` is dropped entirely. MLlib has no TargetEncoder and the
    corridor has 19,953 levels, so one-hot encoding it is not viable. Per §5b
    this is the Tier-2 gap from §10 showing up in practice — a finding to
    report, not a defect to paper over — which is why the exclusion is a named
    constant here rather than an omission somewhere in the script.
  - Rows produced here are the same shape as `evaluate.evaluate()` rows, so the
    Spark baseline sorts into the one leaderboard next to the sklearn sweep
    instead of living in a parallel table.
"""

import numpy as np

from .features import CATEGORICAL_COLUMNS

# Dropped from the MLlib baseline entirely — see module docstring.
MLLIB_EXCLUDED_COLUMNS = ("od_corridor",)

# The low-cardinality categoricals MLlib can afford to one-hot: the Phase-2
# categorical list minus the corridor. Derived from features.CATEGORICAL_COLUMNS
# so the two cannot drift apart.
MLLIB_CATEGORICAL_COLUMNS = tuple(
    c for c in CATEGORICAL_COLUMNS if c not in MLLIB_EXCLUDED_COLUMNS
)

# The three metrics every model in this project reports (evaluate.compute_metrics).
METRIC_KEYS = ("mae", "rmse", "r2")


def split_column_groups(columns) -> tuple[list[str], list[str]]:
    """Split a feature-frame's columns into (categorical, numeric) for MLlib.

    Categoricals go through StringIndexer + OneHotEncoder; numerics pass
    straight into the VectorAssembler, unscaled — GBT is a tree, so scaling
    buys nothing.

    Both groups come back **sorted**, not in DataFrame order. VectorAssembler
    input order is what fixes feature-importance indices, so deriving it from
    however the caller's columns happen to be arranged would silently relabel
    importances when an upstream frame is rebuilt in a different order.

    Columns not present are simply absent from the result, so reduced frames
    (smoke runs, the duration ablation) need no special-casing.
    """
    kept = [c for c in columns if c not in MLLIB_EXCLUDED_COLUMNS]
    categorical = sorted(c for c in kept if c in MLLIB_CATEGORICAL_COLUMNS)
    numeric = sorted(c for c in kept if c not in MLLIB_CATEGORICAL_COLUMNS)
    return categorical, numeric


def fold_metrics_to_row(name: str, fold_metrics, fit_times) -> dict:
    """Fold-wise RegressionEvaluator results -> one leaderboard row.

    `fold_metrics` is one dict per fold with mae/rmse/r2 keys (the
    `evaluate.compute_metrics` shape); `fit_times` is the matching wall time
    per fold.

    Spread is population std (ddof=0) because `evaluate()` takes
    `ndarray.std()`, whose default is ddof=0. Using a sample std here would
    make the Spark row look systematically tighter or looser than every
    sklearn row sharing the table, for no reason a reader could see.
    """
    fold_metrics = list(fold_metrics)
    fit_times = list(fit_times)

    if not fold_metrics:
        raise ValueError("fold_metrics is empty — nothing to summarise")
    if len(fold_metrics) != len(fit_times):
        raise ValueError(
            f"got {len(fold_metrics)} fold metrics but {len(fit_times)} fit "
            "times — zipping these would silently drop a fold"
        )

    row = {"model": name}
    for key in METRIC_KEYS:
        values = np.array([fold[key] for fold in fold_metrics], dtype=float)
        row[f"{key}_mean"] = float(values.mean())
        row[f"{key}_std"] = float(values.std())
    row["fit_time_s"] = float(np.mean(fit_times))
    return row
