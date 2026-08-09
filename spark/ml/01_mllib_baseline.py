"""
Phase 4b — the Spark MLlib GBT baseline (plan §5b).

The DE-portfolio counterpart to the sklearn sweep: one Spark-native model,
trained on **the same rows the sweep trained on**, so the only difference
between its leaderboard row and sklearn's is the stack itself.

Reads `spark/ml/data/sample_work_train.parquet` — written by
`01_run_sweep.py --write-train`. That file is not a convenience: `make_holdout`
is `sklearn.train_test_split` and seeds do not transfer across libraries, so
Spark cannot re-derive those 612,608 rows at any seed. Reading the written
split is the only way the two stacks share a train set, and it keeps the sealed
153,153-row holdout untouched by every model in the project.

The features arrive already derived (the split was written from
`features.build_features` output), so this script does **no** feature
engineering — no second implementation of the cyclic-hour or temp-band
transforms to drift against the pandas ones. That changes if a future run reads
a raw prep sample instead; then the transforms come back and want a parity test.

Run (from repo root):

    # full baseline on the sweep's train split
    .venv/bin/python spark/ml/01_mllib_baseline.py

    # smoke: wiring check, small subset, 2 folds
    .venv/bin/python spark/ml/01_mllib_baseline.py --limit-rows 50000 --folds 2 \
        --max-iter 5 --tag mllib_smoke

Outputs (per run, keyed by --tag):
    spark/ml/results/leaderboard_<tag>.csv    one row, same shape as sklearn rows
    spark/ml/results/sweep_<tag>.json         run metadata + provenance,
                                              same convention as 01_run_sweep.py

Design notes:
  - `od_corridor` is dropped. MLlib has no TargetEncoder and the corridor has
    19,953 levels, so one-hot encoding it is not viable. Per §5b this is the
    §10 Tier-2 gap showing up in practice — a finding to report. It is dropped
    by a named constant in `src/mllib.py`, not quietly here.
  - Folds are k random groups at a fixed seed. They are NOT the sweep's folds:
    sklearn's `KFold(seed=42)` and anything Spark does at `seed=42` partition
    differently, because seeds do not cross library boundaries. Same k, same
    train pool, different membership — noise at the observed ~$0.002 fold SE,
    but not the equivalence an earlier draft of §5b claimed.
  - The holdout is never read here, let alone scored. It is scored once, in
    Phase 5, after the champion is chosen (§4a).
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

# Import the unit-tested modules by their package path (repo root on sys.path).
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import (  # noqa: E402
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import GBTRegressor  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

from spark.ml.src.evaluate import RANDOM_STATE  # noqa: E402
from spark.ml.src.features import TARGET  # noqa: E402
from spark.ml.src.mllib import (  # noqa: E402
    METRIC_KEYS,
    MLLIB_EXCLUDED_COLUMNS,
    fold_metrics_to_row,
    split_column_groups,
)

DATA_DIR = REPO_ROOT / "spark" / "ml" / "data"
RESULTS_DIR = REPO_ROOT / "spark" / "ml" / "results"
DEFAULT_INPUT = DATA_DIR / "sample_work_train.parquet"

FOLD_COL = "_fold"


def model_name(input_path: Path, n_rows: int) -> str:
    """Leaderboard label carrying the pool it was trained on.

    The row shares `evaluate()`'s key set so it sorts into the one leaderboard
    beside sklearn rows — which means there is no column to record the sample
    in. Per §5b the pool goes in the model string instead: adding a field would
    break the shared-leaderboard contract. Rows are stamped from the actual row
    count, so a `--limit-rows` smoke run cannot pass itself off as the real one.
    """
    stem = input_path.stem.removeprefix("sample_").removesuffix("_train")
    return f"mllib_gbt@{stem}{n_rows // 1000}k"


def build_pipeline(categorical, numeric, max_iter: int, max_depth: int) -> Pipeline:
    """The §5b pipeline: index + one-hot the categoricals, assemble, GBT.

    Numerics are assembled raw — GBT splits rather than scales, so a scaler
    buys nothing and would only make the model harder to reason about.

    `handleInvalid="keep"` on the indexer sends an unseen category to its own
    bucket. sklearn's `handle_unknown="ignore"` instead encodes it as all
    zeros. Neither crashes and neither should ever fire here — boroughs and
    service_type are closed sets from the zone lookup — but they are not the
    same behaviour, so it is written down rather than assumed away.
    """
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", dropLast=False)
        for c in categorical
    ]
    assembler = VectorAssembler(
        inputCols=[*numeric, *[f"{c}_ohe" for c in categorical]],
        outputCol="features",
    )
    gbt = GBTRegressor(
        labelCol=TARGET,
        featuresCol="features",
        maxIter=max_iter,
        maxDepth=max_depth,
        seed=RANDOM_STATE,
    )
    return Pipeline(stages=[*indexers, *encoders, assembler, gbt])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                    help="train split written by 01_run_sweep.py --write-train")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--seed", type=int, default=RANDOM_STATE)
    ap.add_argument("--max-iter", type=int, default=100,
                    help="GBT boosting rounds")
    ap.add_argument("--max-depth", type=int, default=5)
    ap.add_argument("--limit-rows", type=int, default=None,
                    help="seeded row subset for a fast wiring check")
    # Measured peak heap on the full 612,608-row split is 0.82 GiB with the
    # cached frame at 53 MB and no disk spill, so 2g is generous. A larger
    # ceiling only gives the JVM permission to grow into swap.
    ap.add_argument("--driver-memory", default="2g")
    # Not local[*]: leaving cores free keeps the machine usable for the ~10
    # minutes this takes, and the job is not CPU-starved at 8.
    ap.add_argument("--cores", type=int, default=8,
                    help="local Spark threads (default 8; 0 = all cores)")
    ap.add_argument("--tag", default="mllib_gbt")
    args = ap.parse_args()

    if not args.input.exists():
        raise SystemExit(
            f"[error] {args.input} not found — write it first with:\n"
            "        python spark/ml/01_run_sweep.py --write-train "
            "--only ridge --folds 2 --tag worksplit"
        )

    spark = (
        SparkSession.builder.appName("mllib-gbt-baseline")
        .master("local[*]" if args.cores == 0 else f"local[{args.cores}]")
        .config("spark.driver.memory", args.driver_memory)
        # 200 shuffle partitions is the cluster default and pure overhead on a
        # single machine at this row count.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(str(args.input))
        if args.limit_rows is not None:
            # Plain head, not a re-sample: the file was written in
            # train_test_split's shuffled order, so the first n rows are
            # already an unbiased subset and an orderBy(rand()) would cost a
            # full shuffle to buy nothing.
            df = df.limit(args.limit_rows)

        feature_columns = [c for c in df.columns if c != TARGET]
        categorical, numeric, unrecognised = split_column_groups(feature_columns)

        # Say what was dropped. A silent drop and a silent include are both
        # ways of not telling you; the deliberate exclusion is reported apart
        # from the anomalies so a documented finding is not buried in noise.
        print(f"[cols] categorical ({len(categorical)}): {categorical}")
        print(f"[cols] numeric     ({len(numeric)}): {numeric}")
        print(f"[cols] excluded by design: {list(MLLIB_EXCLUDED_COLUMNS)}")
        if unrecognised:
            print(f"[cols] !! UNRECOGNISED, not used as features: {unrecognised}")

        # Assign folds once and pin them. rand() is only stable if the frame is
        # not recomputed, and every fold filter below would otherwise re-evaluate
        # it — silently reshuffling the split between folds.
        df = df.withColumn(
            FOLD_COL, (F.rand(seed=args.seed) * args.folds).cast("int")
        ).cache()
        n_rows = df.count()  # materialises the cache, fixing the fold assignment

        name = model_name(args.input, n_rows)
        print(f"[data] {args.input.name}: {n_rows:,} rows -> {name}")
        print(f"[cv]   {args.folds} folds, seed={args.seed}, "
              f"GBT maxIter={args.max_iter} maxDepth={args.max_depth}")

        pipeline = build_pipeline(categorical, numeric, args.max_iter, args.max_depth)
        evaluators = {
            key: RegressionEvaluator(
                labelCol=TARGET, predictionCol="prediction", metricName=key
            )
            for key in METRIC_KEYS
        }

        fold_metrics, fit_times = [], []
        started = time.time()
        for fold in range(args.folds):
            train = df.filter(F.col(FOLD_COL) != fold).drop(FOLD_COL)
            test = df.filter(F.col(FOLD_COL) == fold).drop(FOLD_COL)

            t0 = time.time()
            model = pipeline.fit(train)
            fit_times.append(time.time() - t0)

            predictions = model.transform(test)
            metrics = {k: ev.evaluate(predictions) for k, ev in evaluators.items()}
            fold_metrics.append(metrics)
            print(f"[fold {fold}] mae={metrics['mae']:.4f} "
                  f"rmse={metrics['rmse']:.4f} r2={metrics['r2']:.4f} "
                  f"fit={fit_times[-1]:.1f}s")
        elapsed = time.time() - started

        row = fold_metrics_to_row(name, fold_metrics, fit_times)

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        board_path = RESULTS_DIR / f"leaderboard_{args.tag}.csv"
        import pandas as pd  # local: keeps the Spark path pandas-free until now

        pd.DataFrame([row]).to_csv(board_path, index=False)

        meta = {
            "tag": args.tag,
            "model": name,
            "stack": "spark-mllib",
            "input": str(args.input.relative_to(REPO_ROOT)),
            "rows": n_rows,
            "features": {"categorical": categorical, "numeric": numeric},
            "excluded_by_design": list(MLLIB_EXCLUDED_COLUMNS),
            "unrecognised_columns": unrecognised,
            "folds": args.folds,
            "seed": args.seed,
            # Same k and same train pool as the sklearn sweep, but NOT the same
            # fold membership — seeds do not transfer across libraries.
            "folds_match_sklearn_membership": False,
            "gbt": {"maxIter": args.max_iter, "maxDepth": args.max_depth},
            "elapsed_s": round(elapsed, 1),
            "spark": spark.version,
            "python": platform.python_version(),
            "platform": platform.platform(),
        }
        meta_path = RESULTS_DIR / f"sweep_{args.tag}.json"
        meta_path.write_text(json.dumps(meta, indent=2))

        print(pd.DataFrame([row]).to_string(index=False))
        print(f"[write] {board_path}")
        print(f"[write] {meta_path}")
        print(f"[time] {elapsed:.1f}s")
        print("MLLIB BASELINE OK")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
