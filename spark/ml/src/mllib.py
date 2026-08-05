"""Pure helpers for the Phase-4b Spark MLlib GBT baseline (plan §5b).

Deliberately free of any `pyspark` import. The fit itself is a script run
(01_mllib_baseline.py) like Phase 1, but the decisions *around* the fit — which
columns get one-hot encoded, which pass through raw, and how a Spark result
becomes a leaderboard row — are ordinary data transformations, so they live
here and are unit-tested on the host venv without a SparkSession.

Three contracts matter:

  - `od_corridor` is dropped entirely. MLlib has no TargetEncoder and the
    corridor has 19,953 levels, so one-hot encoding it is not viable. Per §5b
    this is the Tier-2 gap from §10 showing up in practice — a finding to
    report, not a defect to paper over — which is why the exclusion is a named
    constant here rather than an omission somewhere in the script.
  - Column groups are **allowlists**, mirroring
    `preprocess.build_preprocessor`'s `remainder="drop"`. Both stacks must
    discard the same columns or they are not training on the same feature set,
    and the comparison §5b exists to make stops meaning anything. An earlier
    catch-all ("numeric is everything not categorical") sent any unexpected
    column straight into the VectorAssembler — a stray pandas index would have
    become a model feature, and a re-added `fare_amount` would have been
    trained on while sklearn silently dropped it.
  - Dropped columns are **named, not swallowed**. A silent drop and a silent
    include are both ways of not telling you, so the unrecognised names come
    back for the script to print. Deliberate exclusions are reported
    separately from anomalies: burying `od_corridor` in a list of surprises
    would bury the §5b finding too.

Rows produced here are the same shape as `evaluate.evaluate()` rows, so the
Spark baseline sorts into the one leaderboard next to the sklearn sweep instead
of living in a parallel table.
"""

import numpy as np

from .features import CATEGORICAL_COLUMNS
from .preprocess import BINARY_COLUMNS, NUMERIC_COLUMNS

# Dropped from the MLlib baseline entirely — see module docstring.
MLLIB_EXCLUDED_COLUMNS = ("od_corridor",)

# The low-cardinality categoricals MLlib can afford to one-hot: the Phase-2
# categorical list minus the corridor. Derived from features.CATEGORICAL_COLUMNS
# so the two cannot drift apart.
MLLIB_CATEGORICAL_COLUMNS = tuple(
    c for c in CATEGORICAL_COLUMNS if c not in MLLIB_EXCLUDED_COLUMNS
)

# Everything that goes into the VectorAssembler unscaled. sklearn keeps numerics
# and binaries apart because StandardScaler should not touch a 0/1 flag; GBT
# splits rather than scales, so the distinction is meaningless here and the two
# lists are unioned. Derived, not retyped, for the same reason as above —
# and the union is stable if a column ever moves between the two sklearn lists.
MLLIB_NUMERIC_COLUMNS = tuple(dict.fromkeys([*NUMERIC_COLUMNS, *BINARY_COLUMNS]))

# The three metrics every model in this project reports (evaluate.compute_metrics).
METRIC_KEYS = ("mae", "rmse", "r2")


def split_column_groups(columns) -> tuple[list[str], list[str], list[str]]:
    """Split a feature frame's columns into (categorical, numeric, unrecognised).

    Categoricals go through StringIndexer + OneHotEncoder; numerics pass
    straight into the VectorAssembler, unscaled — GBT is a tree, so scaling
    buys nothing. **Unrecognised columns go nowhere near the model**; they are
    returned so the caller can say what it ignored.

    All three lists come back **sorted**, not in DataFrame order.
    VectorAssembler input order is what fixes feature-importance indices, so
    deriving it from however the caller's columns happen to be arranged would
    silently relabel importances when an upstream frame is rebuilt in a
    different order. `unrecognised` is sorted so a warning reads the same twice.

    Absent is not the same as unknown. Columns that simply aren't there are
    absent from the result and reported nowhere, so reduced frames (smoke runs,
    the duration ablation) need no special-casing and produce no warning — a
    warning that fires on every ordinary run is one people stop reading.
    Likewise `MLLIB_EXCLUDED_COLUMNS` is dropped without being called
    unrecognised: that exclusion is a documented §5b finding, not a surprise.
    """
    kept = [c for c in columns if c not in MLLIB_EXCLUDED_COLUMNS]
    categorical = sorted(c for c in kept if c in MLLIB_CATEGORICAL_COLUMNS)
    numeric = sorted(c for c in kept if c in MLLIB_NUMERIC_COLUMNS)
    known = set(MLLIB_CATEGORICAL_COLUMNS) | set(MLLIB_NUMERIC_COLUMNS)
    unrecognised = sorted(c for c in kept if c not in known)
    return categorical, numeric, unrecognised


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
