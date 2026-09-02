# ML Handoff Context — Urban Mobility Insights → Fare Prediction Model

**Purpose:** Consolidated context for the downstream PyTorch fare prediction pipeline, exported from the Looker Studio dashboard build (Claude on claude.ai) for local development in Claude Code / VSCode.

**Source project:** Urban Mobility Insights — NYC taxi dashboard (Yellow + Green, 2015–2016, 128.8M trips) enriched with daily climate data.

**Generated:** 2026-07-04, at the point the dashboard build was declared complete and ML work began.

---

## 1. Data source

- **GCP project:** `dtc-de-project-492321`
- **Dataset:** `dbt_prod`
- **Primary table:** `fact_trips` (built by dbt from `stg_green_taxi_data` + `stg_yellow_taxi_data`, unioned)
- **Secondary view:** `feature_correlations_v` — precomputed Pearson correlations vs `fare_capped` (see §3)
- **Row count baseline:** 128,781,646 trips (confirmed Day 1; note ANOM-001 below re: `COUNT(DISTINCT tripid)` undercounting — always use `COUNT(*)`)

### Key native columns
| Column | Notes |
|---|---|
| `fare_amount`, `trip_distance` | Raw; use capped versions for modeling (see §4) |
| `fare_capped`, `distance_capped` | p99-capped calculated fields (Looker-side); **replicate cap logic in Python, see §4** |
| `pickup_datetime`, `dropoff_datetime` | TIMESTAMP; native duration = `DATETIME_DIFF(dropoff_datetime, pickup_datetime, MINUTE)` |
| `pickup_borough`, `dropoff_borough` | Includes "EWR" (Newark) — recurring exclusion pattern in dashboard borough charts, evaluate for ML too |
| `service_type` | "Yellow" / "Green" |
| `ratecodeid` | INT64 in BigQuery. Codes 2/3 = airport flat-rate (see §3, ratecodeid finding) |
| `payment_type` | Card vs cash — **not known at trip-start**, inference-time-only feature if used at all |
| `passenger_count` | Contains NULLs (~0.62%), zeros (0.01%), and 7–9 (invalid, exceeds legal capacity) — see ANOM-011 |
| `pickup_hour`, `pickup_dow` | Native or derivable from `pickup_datetime` |
| `tip_amount`, `total_amount`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `extra` | **Leakage fields — see §5** |

### Fields that exist only as Looker Studio calculated fields (NOT in BigQuery — must be reimplemented in Python)
- `temp_band` — CASE on temperature, boundaries **32/50/68/85°F** (Freezing/Cold/Mild/Warm/Hot) — confirmed authoritative boundaries, differ from initial assumption of 32/50/65/80
- `trip_duration_min` — derive via `DATETIME_DIFF`
- `revenue_per_mile` — `fare_capped / distance_capped` — **excluded from ML, see §5**
- `avg_mph` — `distance_capped / (trip_duration_min/60)`, guarded: `IF(trip_duration_min <= 0 OR trip_duration_min > 180, NULL, ...)` — apply same guard if recreating

---

## 2. Confirmed baselines (ground truth — HTML mockup values are NOT ground truth, see below)

**Critical:** A file called `looker_climate_template.html` was used only as a layout/design reference during the dashboard build. It contains fabricated placeholder numbers with no relationship to real data. If Claude Code ever encounters that file or references to it, none of its numeric values should be treated as ground truth. All figures below come from actual BigQuery validation.

| Metric | Value |
|---|---|
| Total trips | 128,781,646 |
| Yellow trips (baseline) | ~100.4M |
| Green trips (baseline) | ~28.4M |
| Yellow avg fare | $12.97 |
| Green avg fare | $12.15 |
| Fare cap (p99), Yellow | $52 |
| Fare cap (p99), Green | $46 |
| Distance cap (p99), Yellow | 18.76 mi |
| Distance cap (p99), Green | 14.19 mi |
| Multi-passenger rate (valid trips only) | 26.5% |

---

## 3. Feature correlations (Pearson r vs `fare_capped`) — CONFIRMED, from `feature_correlations_v`

| Feature | r | \|r\| | ML note |
|---|---|---|---|
| `trip_distance` | 0.9596 | 0.9596 | Dominant predictor |
| `trip_duration_min` | 0.8815 | 0.8815 | Strong but collinear with distance |
| `ratecodeid` (numeric) | 0.2465 | 0.2465 | **Understates true signal** — see below |
| `temperature` (highTemp) | 0.0248 | 0.0248 | Near-zero linear signal |
| `passenger_count` | 0.0179 | 0.0179 | Weakest candidate |
| `pickup_hour` | -0.0178 | 0.0178 | Near-zero linear, but retains value via cyclic encoding for non-linear patterns |
| `pickup_dow` | -0.0040 | 0.0040 | Negligible |

**ratecodeid finding:** treating it as a continuous numeric understates the real signal. The true signal is binary — `ratecodeid IN (2,3)` (airport flat-rate) produces a 4× fare difference (~$52 vs ~$12 avg). **Recommendation: engineer `is_airport_trip` as a binary flag rather than relying on raw numeric or one-hot of all rate codes equally.**

**Precipitation:** confirmed null finding — no measurable effect on any trip metric (fare, duration, or demand). Excluded from feature set entirely; do not re-test unless new data source is introduced.

---

## 4. Feature engineering plan (agreed, five-stage pipeline)

1. **Extraction:** BigQuery → 10% stratified sample (stratify on `service_type` and `temp_band`) → Parquet
2. **Feature engineering:**
   - Cyclic encoding (sin/cos) for `pickup_hour`
   - One-hot encoding: `pickup_borough`, `dropoff_borough`, `service_type` (consider excluding EWR-only trips or flagging separately, per recurring EWR pattern)
   - Ordinal encoding: `temp_band` (Freezing < Cold < Mild < Warm < Hot)
   - Target encoding: OD corridor (pickup zone × dropoff zone)
   - Binary flag: `is_airport_trip` from `ratecodeid IN (2,3)` (see §3)
   - 70/15/15 train/val/test split
3. **PyTorch training** (baseline feedforward net; distance + duration dominate signal)
4. **Evaluation:** MAE, RMSE, R² — overall and broken out by `pickup_borough`, `temp_band`, `pickup_hour`
5. **Revision loop**

### Preprocessing guards to replicate from dashboard (data quality issues confirmed in production data)
- `trip_duration_min`: **no cap applied in dashboard by design** (see ANOM-009), but for ML: cap at p99 (~45 min per NYC taxi norms) and **exclude negative-duration records** (`dropoff_datetime < pickup_datetime` — timestamp errors in source)
- `avg_mph` (if engineered as a feature): guard `duration > 0 AND duration <= 180 minutes` before computing, else NULL/exclude (ANOM-008)
- `passenger_count`: valid range is 1–6 only; NULL, 0, and 7–9 are invalid (~0.64% of dataset). Impute NULLs with mode (=1) if included as a feature (ANOM-011)
- `fare_amount`, `trip_distance`: apply same p99 caps as dashboard (§2 table) — or re-derive p99 from the training sample directly, whichever is more defensible for the paper trail
- Report-level filters applied in dashboard, consider replicating as base filters: `trip_distance > 0`, `fare_amount >= 0` (~89K negative-fare records exist, ~0.07%)
- `tripid` is **not a reliable unique key** — was generated without `service_type` in the surrogate key, causing collisions across Yellow/Green with same vendor+timestamp (ANOM-001). Use `COUNT(*)` logic / do not dedupe on `tripid`.

---

## 5. Leakage exclusions (hard exclusion list — confirmed in v3 plan)

These fields must **not** be used as training features because they are derived from or components of the prediction target (`fare_capped` / `fare_amount`):

- `revenue_per_mile` (derived from `fare_capped`)
- `tip_amount`, `total_amount` (components of/derived from total fare)
- `has_tip`, `tip_pct_of_fare` (derived from `tip_amount`)
- Any other component fields of the fare structure (`mta_tax`, `tolls_amount`, `improvement_surcharge`, `extra`) — these are legitimate to visualize on the dashboard but should not appear as predictors of `fare_capped` since they're set alongside/after the base fare, not causally prior to it. Flag for a judgment call: if modeling `total_amount` instead of base fare, this changes.

`payment_type`: not leakage, but not known at trip-start — only usable if the model is framed as post-trip fare estimation rather than pre-trip prediction. **This connects to the open question from our last exchange (duration/payment as pre-trip vs post-trip features) — resolve before finalizing feature set.**

---

## 6. Full anomaly log (ML-relevant subset, condensed)

| ID | Issue | ML implication |
|---|---|---|
| ANOM-001 | `tripid` surrogate key collides across Yellow/Green (missing `service_type` in key) | Don't dedupe on `tripid`; use `COUNT(*)` |
| ANOM-002 | Corrupted Yellow fare/distance outliers (resolved via p99 cap) | Apply same p99 caps to training data |
| ANOM-003 | ~89K negative fares (~0.07%) | Filter `fare_amount >= 0` |
| ANOM-007 | Precipitation has no measurable effect on fare/demand/duration | Excluded from feature set |
| ANOM-008 | `avg_mph` divide-by-zero / outlier corruption without duration guards | Guard `0 < duration <= 180` before computing speed features |
| ANOM-009 | `trip_duration_min` uncapped, ~3–4% inflation in averages; some negative-duration records from timestamp errors | Cap at p99 (~45 min); exclude negative-duration records |
| ANOM-011 | `passenger_count`: NULLs (~0.62%), zeros (0.01%), invalid 7–9 values (~0.64% total affected) | Valid range 1–6 only; impute NULL with mode=1 if used |
| ANOM-012 | Stray filter caused Page 3 scorecard/chart mismatch | Display-only issue, no ML impact |

Full anomaly detail (root causes, resolutions, discovery dates) available in source project files `2026-07-02-anomaly-tracker.md` if deeper audit trail is ever needed — this table has the condensed ML-relevant version.

---

## 7. Open questions to resolve before finalizing feature set

1. **`trip_duration_min` inclusion:** collinear with distance (r=0.88) and not known at prediction time for a true pre-trip fare estimator. Decide: pre-trip model (exclude) vs. post-trip/estimation model (include) vs. build both and compare.
2. **Model comparison scope:** PyTorch only, or PyTorch + gradient boosting (e.g., XGBoost) baseline for comparison, given distance/duration dominate and a tree-based model may be a useful sanity-check baseline.
3. **EWR handling:** dashboard consistently excludes EWR from borough-level charts (not a real NYC borough). Decide whether to exclude EWR trips from training entirely, or keep with a flag, given airport trips are actually a strong signal (`is_airport_trip`).

---

## 8. Reference — full 10-day dashboard build status (for context only, not needed for ML work)

All 6 dashboard pages complete. Day 9–10 global theming/polish in progress (cosmetic only, no data implications). Active plan doc: `notes/2026-05-24-Dashboard-development-plan-v3.md`. Not needed for the ML pipeline but included here in case Claude Code needs the full provenance trail.
