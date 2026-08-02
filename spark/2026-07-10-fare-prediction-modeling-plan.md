# Fare Prediction — Classical ML Modeling Plan

**Created:** 2026-07-10
**Author:** Sagar Sinha (with Claude Code)
**Companion doc:** `spark/2026-07-04-ml-handoff-context.md` (data provenance, baselines, anomalies, feature correlations)
**Goal:** Refresh the full classical ML toolkit (linear → trees → bagging → boosting → stacking → voting) on a real regression problem, done with correct preprocessing and fair comparison — **then** move to neural nets.

---

## 0. Framing

- **Target:** `fare_capped` (continuous → **regression**)
- **Data flow:** `migration_backup/fact_trips/` (128.8M rows, 204 parquet, **Spark local**) → guards + stratified sample → **pandas / scikit-learn** from here on. Spark ends before modeling starts. **Torch never runs inside Spark.**
- **No GCP needed.** The dbt-built `fact_trips` backup on local disk is the complete model input. The old account's $50 reactivation is not required for any of this work.
- **Metrics:** MAE, RMSE, R² — overall + sliced by `pickup_borough`, `temp_band`, `pickup_hour`.

### Locked decisions (2026-07-10)
1. **Duration framing:** treat `trip_duration_min` like an app's *estimated-duration* input — available at prediction time as an estimate, therefore a **legitimate candidate feature, not leakage**. Final include/exclude is deferred and wired as an on/off **ablation** (run the sweep both ways, report the gap).
2. **EWR (Newark):** keep EWR trips in training; `is_airport_trip` captures the signal. Do not drop them.

---

## 1. Code structure

```
spark/ml/
├── 00_prep_spark.py            # Spark: fact_trips → cleaned → stratified sample parquet
├── data/
│   ├── sample_full.parquet     # ~10% stratified (~12.8M rows) — final refit
│   └── sample_work.parquet     # ~500K–1M rows — fast sweep iteration
├── src/
│   ├── features.py             # feature engineering (pure fns, unit-tested)
│   ├── preprocess.py           # ColumnTransformer builders, scaled + tree variants
│   └── evaluate.py             # shared CV harness + leaderboard (unit-tested)
├── notebooks/                  # one per phase, thin — calls into src/
└── models/                     # persisted champion estimators + fitted preprocessors
```
`src/` holds pure functions (feature math, cap logic, metrics) → **unit-tested per TDD**. Model fitting lives in notebooks (exploration, not TDD-appropriate).

---

## 2. Phase 1 — Data prep (Spark, one script)

Read the 204-file backup, apply §4 guards, derive caps from the data:
- Filter `fare_amount >= 0`, `trip_distance > 0`; exclude negative-duration rows (`dropoff < pickup`)
- `passenger_count` → valid 1–6, impute NULL with mode = 1
- Re-derive **p99 caps** on `fare_amount` / `trip_distance` from the data itself (defensible paper trail, don't hard-code); cap `trip_duration_min` at p99 (~45 min)
- **Stratified 10% sample** on `service_type` × `temp_band` → `sample_full.parquet`
- Also cut `sample_work.parquet` (~500K–1M rows) for fast iteration
- **Sanity gate:** `COUNT(*)` ≈ 128,781,646 pre-sample; §3 correlations survive the sample

**Output:** two clean sample parquets. Spark's job is done.

---

## 3. Phase 2 — Feature engineering + leakage-safe preprocessing

| Type | Fields | Encoding |
|---|---|---|
| Numeric | `trip_distance`, `temperature`, `passenger_count` | scale (linear/NN only) |
| Numeric (ablation) | `trip_duration_min` | on/off flag; scale (linear/NN only) |
| Cyclic | `pickup_hour` | sin/cos |
| Ordinal | `temp_band` | Freezing < Cold < Mild < Warm < Hot |
| One-hot | `pickup_borough`, `dropoff_borough`, `service_type` | OHE |
| Binary | `is_airport_trip` = `ratecodeid IN (2,3)` | flag (keeps EWR signal) |
| High-card | OD corridor (pickup zone × dropoff zone) | **target encoding, cross-fitted** |

**Leakage discipline (the make-or-break part):**
- **Split first** (70/15/15, stratified), then fit every transformer on **train only** — inside sklearn `Pipeline` / `ColumnTransformer` so leakage is structurally impossible.
- **Target encoding must be cross-fitted** (out-of-fold, `category_encoders` or manual KFold). A naive group-mean leaks the target.
- **Hard exclusions (§5 leakage list):** `tip_amount`, `total_amount`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `extra`, `revenue_per_mile`, `has_tip`, `tip_pct_of_fare`. `payment_type` excluded (not known pre-trip).
- **Scaling only where needed:** build two preprocessor variants — scaled (linear/SVM/NN) and raw (trees).

---

## 4. Phase 3 — Evaluation harness (build BEFORE any model)

One `evaluate(model, X, y, cv)` that every model plugs into: **identical KFold folds, identical metrics**, returns a row for a running **leaderboard** DataFrame. This is what turns the sweep into a fair comparison instead of anecdotes. Unit-test the metric functions.

---

## 4a. Data splitting policy (decided 2026-08-01)

**Supersedes the 70/15/15 split named in §3.** That was written before the CV harness
existed; a fixed validation slice is strictly worse than the 5-fold CV we now have, which
validates against all 612K training rows instead of 115K and exposes fold-to-fold spread.

**Adopted — stratified 80/20 holdout + 5-fold CV inside the 80%:**

- `evaluate.make_holdout(X, y, test_size=0.2)` carves the split **before** the sweep sees
  the data. On `sample_work`: 612,609 train / 153,152 sealed test.
- Stratified on `service_type` × `temp_band_ord` — the same key `00_prep_spark.py`
  sampled with, so the smallest stratum (Green/Freezing, ~1.5%) can't skew.
- Deterministic under `RANDOM_STATE=42`; the sealed rows are re-derivable from the
  sample file + seed + fraction (recorded in `results/sweep_<tag>.json`), so no second
  copy of the data is persisted.
- `01_run_sweep.py` seals it by default and **never scores or prints it**. `--no-holdout`
  exists for wiring smoke runs only.

**Holdout discipline — the part no code enforces.** The test split is scored **exactly
once**, in Phase 5, after the champion is chosen on CV evidence alone. Every decision
informed by a test score makes the final number optimistic: with 15 models separated by
~$0.005 MAE and SE ≈ $0.002, picking the winner *on the holdout* would bias it by roughly
the largest of 15 noise draws (~$0.005) — the same size as the real gap between the top
two models. Tune, ablate and compare freely on CV; touch the holdout at the end.

### Deferred — temporal test set (next time the prep runs)

The 80/20 above is a **random** split, but the deployment case is predicting a fare for a
trip happening *now* from a model trained on *past* trips. A random split lets 2016-06
inform a 2016-03 prediction, which is optimistic relative to reality by an unmeasured
amount. Rate cards didn't change across 2015–16 and a metered fare is close to
deterministic in distance and time, so drift is *probably* small — "probably" being
exactly what a temporal holdout would replace with a number.

**Blocked on data, not effort:** `select_model_columns` (`00_prep_spark.py`) consumes
`pickup_datetime` into `pickup_hour` / `pickup_dow` and drops it, so the samples carry no
date to split on.

When the prep is next re-run:

1. Add `pickup_datetime` (or just a `pickup_month` key) to the `keep` list.
2. Carve a second test set: the **last 2 of 24 months (2016-11, 2016-12, ≈8%)**.
3. Score the Phase-5 champion on **both** test sets and report the pair.
   - **Agreement** → the random split was safe, and you can say so with evidence rather
     than assertion.
   - **Divergence** → real temporal drift, quantified.

Either outcome is a reportable result. This is additive — it does not replace the random
holdout, and doing the random holdout now costs nothing against it.

---

## 5. Phase 4 — The model sweep

All models wrapped in `Pipeline(preprocessor, model)`, all scored through the Phase-3 harness, run on `sample_work` for speed:

| Family | Models |
|---|---|
| **Linear** | OLS, Ridge, Lasso, ElasticNet |
| **Single tree** | DecisionTreeRegressor (interpretable baseline) |
| **Bagging** | RandomForest, ExtraTrees, `BaggingRegressor` |
| **Boosting** | sklearn GradientBoosting, XGBoost, LightGBM, CatBoost (native categoricals) |
| **Stacking** | `StackingRegressor`: bases = {RF, XGB, Ridge}, meta = Ridge |
| **Voting** | `VotingRegressor` over top 2–3 |

**Predict the result first, then verify:** linear underfits the airport non-linearity; single tree overfits; RF/boosting lead; stacking/voting eke out a small gain. Confirming that narrative *is* the exercise.

---

## 5b. Phase 4b — Spark MLlib baseline (D3, adopted 2026-07-30)

One Spark-native model trained on `sample_full.parquet` (12.75M rows) — the DE-portfolio
counterpart to the sklearn sweep. Runs **after** Phase 4 so there is a locked champion to
compare against.

**Model:** `pyspark.ml.regression.GBTRegressor` — the strongest thing MLlib offers, so the
head-to-head is meaningful rather than a strawman. (`LinearRegression` is a one-line
addition if a linear floor is wanted too; not required.)

**Script:** `spark/ml/01_mllib_baseline.py`, following the `00_prep_spark.py` convention —
a parameterizable Spark script, not a notebook. Pure helpers (column-group builders, the
metrics→leaderboard-row adapter) go in `src/` and are **unit-tested per TDD**; the fit
itself is a script run, like Phase 1.

**Pipeline stages** (`pyspark.ml.Pipeline`):
1. `StringIndexer` + `OneHotEncoder` on `service_type`, `pickup_borough`, `dropoff_borough`
2. numerics passed through raw — GBT is a tree, no scaling needed
3. `VectorAssembler` → single `features` vector
4. `GBTRegressor(labelCol="fare_capped", seed=42)`

**`od_corridor` is dropped for this baseline.** MLlib has no `TargetEncoder` and 19,953
one-hot columns is not viable — this is the Tier-2 gap from §10 showing up in practice, and
naming it is part of the finding, not a defect to paper over.

**Fair comparison (the part that matters):** an MLlib model minus the corridor feature
versus a sklearn model with it is not a like-for-like result. So report **three** rows:

| Row | Stack | Rows | `od_corridor` |
|---|---|---|---|
| Phase-4 champion | sklearn | `sample_full` | ✅ target-encoded |
| Champion, corridor dropped | sklearn | `sample_full` | ❌ |
| MLlib GBT | Spark | `sample_full` | ❌ |

Row 2 vs row 3 is the honest stack comparison; row 1 vs row 2 quantifies what the
target-encoded corridor is actually worth. Both are worth reporting.

**Metrics + folds:** `RegressionEvaluator` for mae/rmse/r2 — the same three metrics — with
k-fold at `seed=42` matching `make_cv()`. Results adapt into the existing `leaderboard()`
as ordinary rows, so everything still lands in one table.

**Expected outcome, stated up front:** the MLlib GBT lands close to but behind the sklearn
boosting champion, mostly from the missing corridor feature and MLlib's weaker GBT
implementation — and it takes materially longer per fit at 12.75M rows than sklearn does at
765K. Confirming *that* is the point: it demonstrates the tool at the scale where it earns
its keep, and documents why the sweep itself doesn't live there.

---

## 6. Phase 5 — Tune + diagnose

- Hyperparameter search (`RandomizedSearchCV` / Optuna) on top 2–3 only
- **Refit champion on `sample_full`** (the 12.8M-row sample)
- Residual plots, learning curves, feature importance + **SHAP** on champion
- Slice metrics by borough / temp_band / hour → final leaderboard
- Run the **duration on/off ablation** and report the gap

---

## 7. Phase 6 — Switch to neural nets (only after classical champion locked)

- PyTorch feedforward, with **embedding layers for high-cardinality categoricals** (OD corridor) instead of target encoding — the one place a net genuinely adds value
- Head-to-head vs the tree champion on the same held-out test set
- Single-machine training on the in-memory sample — no distributed Spark/torch

---

## 8. Cross-cutting principles

1. **Compute tiers:** sweep on `sample_work` (~500K–1M) for fast iteration; refit winner on `sample_full` (~12.8M). Avoids minutes-per-fit × dozens of models.
2. **Reproducibility:** fixed `random_state` everywhere; persist samples + fitted preprocessors so the leaderboard is regenerable.
3. **Leakage-safe by construction:** all preprocessing inside Pipelines fit on train folds only; target encoding cross-fitted.
4. **Fair comparison:** every model through the one harness, same folds, same metrics.

---

## 9. Prerequisite: clear the disk barrier

Currently 92% full / ~36 GB free. Reclaim ~10 GB of dead FHVHV homework (untracked, unrelated to the 2015–16 fare pipeline) before Spark needs spill headroom:

| Path | Size | Action |
|---|---|---|
| `spark/test_data/` (fhvhv 2023) | 3.5 G | delete |
| `spark/data/` (old raw/pq) | 2.4 G | verify then delete |
| `spark/fhvhv_tripdata_2021-01.csv` + `.gz` | 0.8 G | delete |
| `spark/fhvhv/`, `spark/fhvhv_susbet/` | 0.6 G | delete |
| **`migration_backup/`** | 7.1 G | **KEEP — crown jewels** |

---

## 10. Spark vs scikit-learn — scope decision (2026-07-30)

Revisited after Phases 1–3 landed: *how much of this pipeline should be PySpark?* Audit of
what exists today — only `00_prep_spark.py` imports `pyspark`; `features.py` is pandas,
`preprocess.py` and `evaluate.py` are sklearn.

### The three tiers

**Tier 1 — converts cleanly, 1:1.** Every function in `features.py` is a column expression
with no cross-row state:

| pandas | PySpark |
|---|---|
| `pd.to_numeric(...).astype(float64)` | `F.col(c).cast("double")` |
| `np.sin(2π·hour/24)` | `F.sin(2*pi*F.col("pickup_hour")/24)` |
| `.map({band: i})` | `F.when(...).otherwise(...)` chain |
| `pickup_zone + "→" + dropoff_zone` | `F.concat_ws("→", ...)` |
| `.drop(columns=[...])` | `.drop(*cols)` |

`cast_decimal_columns` becomes unnecessary in Spark — Decimal-as-object is a *pandas*
artifact of reading `decimal(38,9)`; Spark reads it as native `DecimalType`.

**Tier 2 — converts with real rework.** `StandardScaler`/`OneHotEncoder` exist in
`pyspark.ml.feature`, but the shape changes: `ColumnTransformer` → `VectorAssembler` + a
stage chain, and OHE becomes two stages (`StringIndexer` → `OneHotEncoder`).
**The blocker is `TargetEncoder` — Spark ML has no equivalent.** Cross-fitting 19,953 OD
corridors by hand (fold ids → group means over each fold's complement → join back with
smoothing) is the one place a bug is *silent leakage* that inflates R² unnoticed. In
`evaluate.py`, metrics map to `RegressionEvaluator`, but `cross_validate`'s per-fold score
arrays and `fit_time` need hand-rolling, and the identical-folds-for-every-model guarantee
(the entire point of Phase 3) gets **harder** to enforce, not easier.

**Tier 3 — doesn't convert.** MLlib coverage against the §5 sweep: ✅ LinearRegression
(`elasticNetParam` covers OLS/Ridge/Lasso/ElasticNet), DecisionTree, RandomForest;
⚠️ `GBTRegressor` (weaker than sklearn GB), XGBoost via `xgboost.spark`, LightGBM via
SynapseML (heavy JVM dep), CatBoost (fragile Spark package); ❌ **ExtraTrees,
BaggingRegressor, StackingRegressor** — roughly a third of the sweep lost or hand-built.

### The cost nobody mentions

`sample_work.parquet` is 765K rows. On a single laptop, Spark on 765K rows is **slower**
than pandas — JVM startup, serialization and shuffle are pure overhead with no cluster to
amortize against. And the 110 unit tests currently run in milliseconds because they are
pure functions over small frames; every one would need a `SparkSession` fixture and go to
seconds. The TDD loop that carried Phases 1–3 gets materially worse.

### Decisions

- **D1 — ADOPTED: move feature engineering into the Spark prep.** Port the `features.py`
  column logic into `00_prep_spark.py` so it runs on all 128.78M rows **before** sampling,
  not on the sample after. This is the one conversion that plays to Spark's strength: the
  samples then carry engineered features, and training on far more than 765K rows stays
  open. Phase 2's split/encoding discipline is unaffected — only the deterministic column
  math moves upstream.
- **D2 — ADOPTED: `preprocess.py`, `evaluate.py` and the Phase-4 sweep stay scikit-learn.**
  Not because Spark can't, but because at 765K rows it is slower, it costs `TargetEncoder`
  and a third of the model families, and it buys nothing demonstrable. §0's "Spark ends
  before modeling starts" stands.
- **D3 — ADOPTED (2026-07-30): one MLlib baseline on `sample_full`.** A single Spark-native
  model alongside the sklearn sweep — the skill demonstrated at a data size where Spark is
  genuinely the right tool, without rewriting three working modules. Specified as **Phase 4b**
  in §5b below.

**Naming note:** `spark/` is the batch-processing + ML home (inherited from Phase 3's
rename of `05_batch_processing/`), not a claim that everything inside is Spark. Renaming
now would churn Docker mounts and import paths for cosmetics — not worth it.

---

## Status
- [x] Plan reviewed by Sagar
- [x] **Spark-vs-sklearn scope decision — §10, 2026-07-30.** D1 adopted (feature
      engineering moves into the Spark prep, pre-sample), D2 adopted (preprocessing, CV
      harness and sweep stay sklearn), **D3 adopted** — single MLlib GBT baseline on
      `sample_full`, specified as Phase 4b in §5b. Supersedes nothing in §0–§9 except the
      placement of `features.py` logic; no phase is invalidated.
- [~] Phase 0: Spark verified (4.1.2 on Py3.13/Java21) + FHVHV disk cleanup **pending user `rm`**; `spark/data/` (2.4G redundant) pending user OK
- [x] **Phase 1: prep script + samples — DONE 2026-07-10.** `spark/ml/00_prep_spark.py` →
      `sample_full.parquet` (12,748,027 rows) + `sample_work.parquet` (765,761) + `prep_stats.json`.
      Verified: raw count 128,781,646 (exact), caps match dashboard (Yellow $52/18.7mi, Green $45/14.15mi),
      avg fares $12.87/$12.02, multi-passenger 26.5% (exact), no nulls, OD cardinality 19,953 zone pairs.
      NOTE: optional climate cols (humidity/windSpeed/visibility) are Decimal→object; cast or drop in Phase 2.
- [x] Phase 2: features + leakage-safe preprocessing — **features.py DONE 2026-07-10 (TDD)**.
      `spark/ml/src/features.py` + `tests/unit/ml/test_features.py` (24 tests, RED→GREEN).
      Decimal→float64 cast locked (humidity/windSpeed/visibility kept as features);
      cyclic hour, temp_band ordinal, od_corridor key, leakage exclusions (incl.
      `fare_amount`, `distance_capped`, `ratecodeid`, raw zones), duration ablation flag.
      Real-data smoke on sample_work: X=(765761, 16), 0 nulls, od cardinality 19,953 ✓.
      **preprocess.py DONE 2026-07-10 (TDD, 17 tests):** scaled + tree ColumnTransformer
      variants; OHE (`handle_unknown="ignore"`); od_corridor via **sklearn TargetEncoder**
      (built-in cross-fitted `fit_transform` — no category_encoders dep needed); column
      lists auto-adapt to the duration ablation. Real-data smoke: both variants →
      (765761, 27) float64, finite, ~1s each; te__od_corridor spans $0–$52 (= yellow cap).
      **Phase 2 COMPLETE** — 94 unit tests green.
- [x] Phase 3: evaluation harness — **DONE 2026-07-10 (TDD, 16 tests).**
      `spark/ml/src/evaluate.py`: `mae/rmse/r2` + `compute_metrics` (single source),
      `make_cv` (KFold shuffle seed=42, reproducibility tested), `evaluate()` →
      leaderboard row (negated-scorer flip tested), `leaderboard()` sorted by RMSE.
      110 unit tests green. Smoke leaderboard (100K rows, 3-fold, duration ON):
      tree_d8 MAE $0.51 / R² .981 · ridge $0.84 / .937 · dummy $6.73 / 0 —
      matches plan §5 prediction (tree > linear ≫ mean). High R² expected with
      duration in (metered fare ≈ f(distance, time)) → ablation will quantify it.
- [~] Phase 4: model sweep — registry + runner built; **first work-sized sweep INVALID**,
      superseded by the 2026-08-01 feature fix below. Re-run pending.
- [x] **Feature-set + split correction — 2026-08-01 (TDD).** The 2026-07-31 work sweep
      returned R² −1,192,374 for `ols`/`ridge` (and −36K / −132K for `elasticnet` /
      `voting`). Root cause: `sample_work` row 533491 carries a corrupt
      **8,003,318-mile** odometer reading ($15.50 fare, 22 min). `features.py` had
      excluded `distance_capped` as a "duplicate" and kept the uncapped
      `trip_distance`, disabling the §2 p99 cap built to neutralise exactly this. With
      KFold(seed=42) the row lands in **fold 0's test split**, so the scaler was fit
      without it (σ=3.59 vs 10,222) and the row transformed to 2,228,170 → a **$9.29M
      prediction**; that single row was 100.00% of fold 0's squared error. Trees were
      immune (split-based), which is why the leaderboard top looked healthy.
      **Fixed:** only the derived half of each raw/derived pair now trains —
      `distance_capped` replaces `trip_distance`, `temp_band_ord` replaces
      `temperature`. X: 16 → **15 features**, matrix 27 → **26 columns**, scaled
      `max|x|` 2,228,170 → **52**. Smoke: `ols`/`ridge` R² **0.969**.
      **Added:** §4a split policy — `evaluate.make_holdout` + `--holdout-frac` /
      `--no-holdout`. 208 unit tests green.
      **Known, untouched:** design matrix is rank 25/27 (full one-hot + intercept =
      dummy trap, cond ≈ 4e15); coefficients unstable across folds. Separate defect.
- [ ] Phase 4b: Spark MLlib GBT baseline on `sample_full` (§5b)
- [ ] Phase 5: tune + diagnose — scores the sealed holdout **once**, at the end (§4a)
- [ ] Phase 6: neural nets
