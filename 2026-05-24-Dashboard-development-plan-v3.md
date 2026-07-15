## Project context

We are building a multi-page Looker Studio dashboard on NYC taxi data (2015–2016) joined with daily climate data. The pipeline is dbt + BigQuery. All analysis is done from `dtc-de-project-492321.dbt_prod`. The primary fact table is `fact_trips`.

**This is v3 of the plan.** It reconciles v2 (2026-05-17) with the visual design mockup in `looker_climate_template.html` (added to project files on 2026-05-24). The HTML mockup is the canonical design reference for layout, colors, typography, and chart selection. Where the v2 plan added analytical depth beyond the mockup, those additions are kept.

We work one day at a time, one piece at a time, as a senior analyst would. Always outline the plan for the current day and wait for approval before executing. After each major step, summarize what was done and what comes next. Never jump ahead.

---

## Stack & key references

| Item | Value |
|---|---|
| BigQuery project | `dtc-de-project-492321` |
| Primary dataset | `dbt_prod` |
| Main fact table | `dtc-de-project-492321.dbt_prod.fact_trips` |
| Staging — Green | `dtc-de-project-492321.dbt_prod.stg_green_taxi_data` |
| Staging — Yellow | `dtc-de-project-492321.dbt_prod.stg_yellow_taxi_data` |
| Zones dim | `dtc-de-project-492321.dbt_prod.dim_zones` |
| Climate staging | `dtc-de-project-492321.dbt_prod.stg_climate_data` |
| Revenue dim | `dtc-de-project-492321.dbt_prod.dim_monthly_zones_revenue` |
| Dashboard tool | Looker Studio (BigQuery connector) |
| Design reference | `looker_climate_template.html` (in project files) |

---

## Validated pipeline baseline (Day 1 — locked, do not re-investigate)

| | Green | Yellow |
|---|---|---|
| Total trips | 28,424,373 | 100,357,273 |
| Total revenue | ~$416M | ~$1.62B |
| Avg fare | $12.15 | $12.97 |
| Climate join match | 100% | 100% |
| Zone join drop rate | 0% | 0% |
| All 24 months present | ✅ | ✅ |

~2.1M trips lost to the `WHERE borough != 'Unknown'` filter in `fact_trips` — by design. Dashboard represents ~98.4% of all staged trips.

**Known data quality issues — already resolved or documented in `2026-05-19-anomaly-tracker.md`:**
- ANOM-001: Surrogate key cross-service collision (deferred; workaround: use Record Count, not COUNT DISTINCT tripid)
- ANOM-002 to ANOM-004: Fare/distance outliers and negatives (resolved via p99 caps + report filters)
- ANOM-005: Green H2 2016 decline (real market signal; visualize on Page 2 daily trend)
- ANOM-006: Unknown borough trips filtered out (by design)

**Outlier capping thresholds (p99, validated):**
| Service | fare p99 cap | distance p99 cap |
|---|---|---|
| Green | $46 | 14.19 mi |
| Yellow | $52 | 18.76 mi |

---

## Design system (locked, applies across all pages)

**Colors (from HTML mockup):**

| Element | Hex | Usage |
|---|---|---|
| Background | `#080C18` | Page background |
| Card background | `#0F1525` | Chart cards |
| Yellow taxi | `#F7C426` | Yellow service type, primary highlight |
| Green taxi (teal) | `#2ECFB1` | Green service type, secondary highlight |
| Coral accent | `#FF6B5B` | Decline indicators, negative deltas |
| Blue accent | `#4C8EFF` | Neutral data series |
| Purple accent | `#9B7FFF` | Neutral data series |
| Muted text | `#8892AB` | Captions, labels |

**Typography:**
- Body / labels: Inter
- Numbers / KPIs / data: DM Mono (monospace)
- Section headers: Syne (or fallback bold sans-serif)

**Layout pattern (per page):**
1. Page tag (top-left) — e.g. "📊 Page 1 · Revenue overview"
2. KPI row (2 or 4 scorecards across the top)
3. Section heading
4. Chart grid (varies — 1×N, 2-column, 3-column, or 2:1)
5. Page filter at top or sidebar

---

## Calculated fields (full inventory)

**Already built (Day 2):**

| Field | Formula |
|---|---|
| `fare_capped` | `IF((service_type="Yellow" AND fare_amount>52) OR (service_type="Green" AND fare_amount>46) OR fare_amount<=0, NULL, fare_amount)` |
| `distance_capped` | `IF((service_type="Yellow" AND trip_distance>18.76) OR (service_type="Green" AND trip_distance>14.19) OR trip_distance<=0, NULL, trip_distance)` |
| `revenue_per_mile` | `IF(distance_capped IS NULL OR distance_capped=0, NULL, fare_capped/distance_capped)` |
| `trip_duration_min` | `DATETIME_DIFF(dropoff_datetime, pickup_datetime, MINUTE)` |
| `weather_condition` (basic) | `IF(precipIntensity > 0, "Rainy", "Dry")` |
| `temp_band` | CASE on `highTemp` → Freezing/Cold/Mild/Warm/Hot |

**Built during Day 4:**

| Field | Formula |
|---|---|
| `pickup_hour` | `HOUR(pickup_datetime)` (Number type) |
| `pickup_weekday_ordered` | `CASE WEEKDAY(pickup_datetime) WHEN 2 THEN "1. Mon" WHEN 3 THEN "2. Tue" WHEN 4 THEN "3. Wed" WHEN 5 THEN "4. Thu" WHEN 6 THEN "5. Fri" WHEN 7 THEN "6. Sat" WHEN 0 THEN "6. Sat" WHEN 1 THEN "7. Sun" END` |

**To build during Day 5 (Weather page):**

| Field | Formula | Purpose |
|---|---|---|
| `weather_condition_refined` | CASE on `precipIntensity` + `lowTemp` → Clear / Light rain / Heavy rain / Snow / Fog (use cloudCover + visibility thresholds for Fog) | Replaces basic 2-level weather_condition |
| `avg_mph` | `distance_capped / (trip_duration_min/60)` | Speed degradation by weather |

**Report-level filters (set in Day 2):**
- `trip_distance > 0`
- `fare_amount >= 0`

---

## 10-day build plan (revised)

| Day | Stage | Focus | Status |
|---|---|---|---|
| 1 | Foundation | Pipeline validation + baselines | ✅ Done |
| 2 | Prep | Calculated fields + data source setup | ✅ Done |
| 3 | Page 1 | Revenue overview | ✅ Done (1 pending fix on Avg Fare by Hour chart) |
| 4 | Page 2 | Demand heatmap | 🟡 In progress (heatmap + top zones done) |
| 5 | Page 3 | Weather impact analysis | ⬜ |
| 6 | Page 4 | Route & corridor deep-dive | ⬜ |
| 7 | Page 5 | Customer behavior (tip analysis) | ⬜ |
| 8 | Page 6 | Fare prediction feature explorer | ⬜ |
| 9–10 | Polish | Theming, cross-filters, final review | ⬜ |

---

## Day 3 pending fix (carry-forward)

**Page 1 — Avg Fare by Hour chart:** increase row limit to 24 and set sort to `pickup_hour` ascending. Removes the "Others" bucket and shows hours 00–23 in order. Defer until after Day 4.

---

## Page 1 — Revenue overview (v3, retroactive)

**Current state:** Built in Day 3. The following changes from HTML mockup are deferred to the Day 9–10 polish phase since the page is already functional:

| Item | Current | v3 target |
|---|---|---|
| KPI count | 5 (incl. Total trips) | 4 (drop Total trips — anchored on Page 2) |
| Monthly revenue trend | SUM(fare_capped) by month | Add secondary line: AVG(revenue_per_mile) |
| Other 4 charts | Built and validated | Keep as-is |

---

## Page 2 — Demand heatmap (Day 4, active)

**Goal:** Show when and where trips cluster. Foundational feature set for the ML fare model.

**Layout (top to bottom):**

1. **2 KPI scorecards**
   - Peak hour trips → MAX of Record Count grouped by (weekday, hour); label with the winning day+hour
   - Lowest demand → MIN of Record Count grouped by (weekday, hour); label with the winning day+hour

2. **Hourly demand heatmap** ✅ Built
   - Rows: `pickup_weekday_ordered` (Mon→Sun)
   - Columns: `pickup_hour` (0→23, numeric type for correct sort)
   - Metric: Record Count
   - Conditional formatting: yellow-to-red color scale

3. **Top 10 pickup zones table** ✅ Built (needs metric tweak)
   - Dimensions: `pickup_zone`, `pickup_borough`
   - Metrics: avg trips/day (Record Count / 730 days), avg `fare_capped`
   - Sort: avg trips/day descending, limit 10
   - "Show others" → Off

4. **Demand by day of week** — horizontal bar chart (NEW)
   - Dimension: `pickup_weekday_ordered`
   - Metric: Record Count (or avg trips/day)
   - Color: yellow for peak (Friday), graduated for others

5. **Daily trip volume trend** — line chart (KEPT from Plan v2)
   - Dimension: `pickup_date` (Day granularity)
   - Metric: Record Count
   - Breakdown: `service_type` (Yellow + Green)
   - Validates ANOM-005 (Green H2 2016 decline)

6. **Page-level filter**
   - Drop-down → `pickup_borough`

**Validation checks:**
- Heatmap peaks Mon–Fri 7–9 AM, 17–19 PM; Fri/Sat 22–02
- Top zones all Manhattan-heavy
- Daily trend shows Yellow >> Green, Green visibly declines in mid-2016

---

## Page 3 — Weather impact analysis (Day 5)

**Goal:** Validate whether climate variables have predictive signal for the ML model.

**Layout:**

1. **4 weather condition cards (KPIs)**
   - Clear → demand multiplier 1.00× (baseline)
   - Light rain → multiplier vs clear baseline
   - Heavy rain → multiplier vs clear baseline
   - Snow → multiplier vs clear baseline
   - Built from `weather_condition_refined` (new calculated field)

2. **Fare uplift by condition** — horizontal bar chart
   - Dimension: `weather_condition_refined`
   - Metric: AVG(`fare_capped`)
   - Compare each condition to Clear baseline

3. **Speed degradation by condition** — horizontal bar chart
   - Dimension: `weather_condition_refined`
   - Metric: AVG(`avg_mph`) (new calculated field)
   - Reveals weather's impact on travel time

4. **Temperature vs avg fare scatter (by borough)**
   - X axis: `highTemp` (or temperature bucket)
   - Y axis: AVG(`fare_capped`)
   - Breakdown/color: `pickup_borough`
   - Add trend line

**Validation checks:**
- Heavy rain should show 30%+ demand uplift (consistent with NYC patterns)
- Snow should show lowest speed (heaviest degradation)
- Temperature should have weak monotonic relationship with fare

---

## Page 4 — Route & corridor deep-dive (Day 6)

**Goal:** Show how revenue flows across geography.

**Layout:**

1. **4 KPI scorecards**
   - Airport revenue share → SUM(`fare_capped`) where `ratecodeid` in (2,3) / total SUM
   - Avg JFK flat fare → AVG(`fare_capped`) where `ratecodeid` = 2
   - Cross-borough share → % trips where pickup_borough ≠ dropoff_borough
   - Intra-Manhattan share → % trips where both are Manhattan

2. **Top OD corridors table**
   - Dimensions: `pickup_zone`, `dropoff_zone`
   - Metrics: Record Count, AVG(`fare_capped`)
   - Sort: Record Count desc, limit 15

3. **Revenue by rate code** — horizontal bar
   - Dimension: `ratecodeid` (with descriptive labels)
   - Metric: SUM(`fare_capped`)

4. **Borough-to-borough flow matrix** (kept from Plan v2)
   - Pivot table: `pickup_borough` × `dropoff_borough`
   - Metric: Record Count
   - Heatmap conditional formatting

---

## Page 5 — Customer behavior (Day 7)

**Goal:** Understand tipping behavior (card-only).

**Layout:**

1. **4 KPI scorecards**
   - Tip conversion rate (card trips with tip > 0)
   - Avg tip amount (where tip > 0)
   - Tip % of fare
   - Multi-passenger rate

2. **Tip rate by hour of day** — bar chart
   - Dimension: `pickup_hour`
   - Metric: % card trips with tip > 0
   - Filter: `payment_type` = 1

3. **Tip rate by borough** — horizontal bar
   - Dimension: `pickup_borough`
   - Metric: % card trips with tip > 0
   - Filter: `payment_type` = 1

4. **Tip distribution by service** (kept from Plan v2)
   - Dimension: tip amount buckets ($0, $0–2, $2–5, $5–10, $10+)
   - Metric: Record Count
   - Breakdown: `service_type`

**Page filter:** `payment_type_description`

---

## Page 6 — Fare prediction feature explorer (Day 8)

**Goal:** Surface outliers, validate fare structure, identify strongest ML features.

**Layout:**

1. **Top predictive features** — horizontal bar (correlation with fare)
   - Pearson |r| of each candidate against `fare_capped`
   - Expected order: trip distance > rate code > duration > OD corridor > weather > hour > temp > weekday
   - Build via one-time BigQuery query, embed as static reference

2. **Trip distance distribution** (kept from Plan v2)
   - Dimension: `distance_capped` bucketed (0-2, 2-4, 4-6, 6-8, 8-10, 10-15 mi)
   - Metric: Record Count

3. **Trip duration distribution** (kept from Plan v2)
   - Dimension: `trip_duration_min` bucketed (0-10, 10-20, 20-30, 30-45, 45-60, 60+ min)
   - Metric: Record Count
   - Decide whether to cap `trip_duration_min` based on what shape shows

4. **Fare component breakdown** — stacked bar
   - Dimension: `service_type`
   - Stacked metrics: SUM(fare), SUM(mta_tax), SUM(tip), SUM(tolls), SUM(improvement_surcharge)

5. **Distance vs fare scatter with trend line** (kept from Plan v2)
   - X: `distance_capped`, Y: `fare_capped`
   - Trend line = visual proxy for model fit

6. **Model feature checklist** — reference table
   - Columns: Looker dimension, Model feature type (numeric / cyclic encode / ordinal cat. / target encode / one-hot)

---

## Day 9–10 — Polish, theming, cross-filters

1. **Apply HTML design system** across all 6 pages (colors, fonts, layout)
2. **Cross-page global filters** — date range, `service_type`, `pickup_borough` filter all 6 pages
3. **Resolve Day 3 pending fix** on Page 1 Avg Fare by Hour chart
4. **Resolve Page 1 v3 changes** — drop Total trips KPI, add Rev/mile secondary line
5. **Stakeholder walkthrough** — every chart title self-explanatory, every KPI validated against Day 1 baseline
6. **BigQuery cost check** — confirm partition filtering on `pickup_date`
7. **ML feature shortlist** — final candidates based on dashboard signals

---

## Versioning notes

- **v1:** Initial plan, pre-Day 1
- **v2 (2026-05-17):** Post-Day 1, locked baselines + calculated fields
- **v3 (2026-05-24):** Reconciled with HTML mockup. Pages 1, 4, 5 minor changes; Page 2 expanded; Page 3 restructured; Page 6 layout simplified.

The HTML mockup (`looker_climate_template.html`) is the canonical design reference. When in doubt about layout, color, or chart selection — defer to the mockup.
