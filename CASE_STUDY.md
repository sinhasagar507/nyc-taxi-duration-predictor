# NYC Taxi Trip Duration Prediction — End-to-End Data Engineering Case Study

> An end-to-end, cloud-native batch data platform that ingests two years of NYC taxi
> records, models them with dbt in BigQuery, and powers both a Looker Studio analytics
> suite and a PySpark trip-duration model — orchestrated by Airflow, tested in CI, and
> reconciled against live Google Cloud infrastructure.

**Stack:** Google Cloud Platform (GCS · BigQuery) · Apache Airflow · dbt Core · PySpark / Spark MLlib · Terraform · Docker · GitHub Actions · pytest

---

## Executive Summary

This project is a production-style data pipeline that turns raw NYC Taxi & Limousine
Commission (TLC) trip data into analytics-ready tables and a machine-learning model for
predicting trip duration. Raw monthly files land in a cloud data lake, become queryable
through a warehouse, get cleaned and reshaped into a dimensional model, and then feed two
consumers: a multi-page business-intelligence dashboard and a Spark ML model.

The platform processes roughly **128.8 million taxi trips** across the 2015–2016 period
(24 months each of yellow and green taxi service), accounting for **~$2.04 billion** in
modeled fare revenue, enriched with daily weather data and **~265 NYC taxi zones**. It is
orchestrated by **9 Apache Airflow DAGs**, validated by a **59-test automated suite**, and
protected by a **CI/CD pipeline** that checks every change before it can reach production.

What makes it more than a tutorial follow-along is the engineering discipline applied on
top of the data work: the entire codebase was reconciled against the *live* cloud project
(not just assumed to be correct), a leaked credential was purged from git history (cutting
the repo from 11 GB to 82 MB), and the whole thing is structured into reversible,
test-gated phases. It demonstrates the full data-engineering lifecycle — ingestion,
storage, warehousing, transformation, orchestration, analytics, ML, infrastructure-as-code,
testing, and CI/CD — end to end.

---

## Architecture at a Glance

```text
TLC source data (2015–2016)
        │
        ▼
  GCS raw storage            ◀── Airflow ingest DAGs (download → upload → clean up)
        │
        ▼
  BigQuery external tables   ◀── Airflow external-table DAGs (CREATE EXTERNAL TABLE)
        │
        ▼
  dbt ELT (staging + marts)  ◀── Airflow dbt DAG  /  GitHub Actions CI
        │
        ▼
  fact_trips + dim tables    ──► dbt_prod dataset
        │
        ├──► Looker Studio dashboard (6-page analytics suite)
        └──► PySpark duration-prediction model
```

| Layer | Technology | Role |
| --- | --- | --- |
| Ingestion / Orchestration | Apache Airflow (Celery + Redis + Postgres, Dockerized) | Schedule and chain the pipeline; backfill 24 months of data |
| Raw storage (data lake) | Google Cloud Storage | Hold raw parquet/CSV exactly as sourced |
| Warehouse | BigQuery (external tables) | Query lake files in place — no duplication |
| Transformation (ELT) | dbt Core + dbt-bigquery | Clean, test, and model into a fact/dim schema |
| Analytics | Looker Studio | Self-serve BI on the modeled data |
| Machine learning | PySpark / Spark MLlib | Predict trip duration from engineered features |
| Infrastructure-as-code | Terraform | Declarative GCP resources (kept as reference) |
| Quality gates | pytest + GitHub Actions | Automated tests + CI/CD before production |

---

## The Problem & Goals

NYC releases detailed, anonymized records for every taxi trip. The headline goal is to
**predict how long a trip will take** from features available at pickup — distance, time of
day, location, weather. But the real objective was broader: build the *entire* data
platform a model like that depends on, using the patterns a production data team would use,
rather than a single notebook.

This was undertaken as the capstone of the
[DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp),
then deliberately pushed well past the course scope — adding a second taxi service (green),
a weather-data join, a multi-page dashboard, a hardened test suite, CI/CD, and a
git-history cleanup. The design priorities were: **reproducibility** (anyone can stand it
up from scratch), **cost-efficiency** (don't pay to store data twice), **correctness**
(every layer is tested), and **honesty** (the code matches the live infrastructure).

---

## Pipeline Walkthrough

### 1. Ingestion — Apache Airflow

Four ingest DAGs pull source data into the cloud data lake:

- `nyc_taxi_gcs_yellow_dag` and `nyc_taxi_gcs_green_dag` download monthly TLC parquet files
  (24 months each, 2015-01 through 2016-12).
- `nyc_climate_gcs_dag` downloads a daily climate CSV and converts it to parquet with
  PyArrow before upload.
- `nyc_taxi_zone_gcs_dag` loads the static taxi-zone lookup table (manual trigger — it is
  reference data with no monthly version).

The trip DAGs run on a monthly schedule (`0 6 1 * *`) with `catchup=True`, so a single
deployment **backfills all 24 historical months** automatically. Each DAG follows the same
task chain — *download → upload to GCS → clean up the local copy* — with GCS uploads using
a 5 MB chunk size to survive slower connections. Crucially, **no project or bucket ID is
hardcoded**: every DAG reads `GCP_PROJECT_ID` and `GCP_GCS_BUCKET` from the environment.

### 2. Lake → Warehouse — BigQuery External Tables

Four more DAGs register the GCS files as **BigQuery external tables** via
`BigQueryInsertJobOperator` running `CREATE OR REPLACE EXTERNAL TABLE`. Each ingest DAG
automatically triggers its matching external-table DAG through Airflow's
`TriggerDagRunOperator`, so loading and registration stay decoupled but linked.

The deliberate choice of **external tables over native (loaded) tables** means BigQuery
queries the parquet/CSV directly in GCS — the data is never copied or duplicated into
warehouse storage. That keeps storage costs minimal and keeps a single source of truth in
the lake, at the cost of slightly slower scans (an acceptable trade for a batch analytics
workload).

### 3. Transformation — dbt Core on BigQuery

The transformation layer is a dbt project (dbt-core / dbt-bigquery 1.11) vendored as a git
submodule. It applies a classic **staging → marts** ELT pattern:

**Staging models (materialized as views):**
- `stg_yellow_taxi_data` and `stg_green_taxi_data` clean each taxi service — deduplicating
  on `(vendorid, pickup_datetime)`, casting types, mapping payment-type codes to readable
  labels via a custom macro, and generating a surrogate `tripid` with
  `dbt_utils.generate_surrogate_key`. Both produce an identical schema so they can be unioned.
- `stg_climate_data` converts Modified Julian Dates to calendar dates and exposes weather
  fields (humidity, visibility, temperature, precipitation, wind, cloud cover).

**Core models (materialized as tables):**
- `fact_trips` — the central fact table. It unions yellow + green trips (tagged by
  `service_type`), inner-joins `dim_zones` twice (pickup and dropoff) to validate geography,
  and left-joins climate data on the pickup date to attach weather context.
- `dim_zones` — the zone dimension, built from a **dbt seed** (`taxi_zone_lookup.csv`,
  ~265 NYC zones across the five boroughs and airports) rather than an external table,
  because the data is small and static.
- `dim_monthly_zones_revenue` — pre-aggregated revenue and trip metrics by zone, month, and
  service type, purpose-built for the dashboard.

Data quality is enforced with dbt **schema tests**: uniqueness and not-null on keys,
relationship tests linking trips to valid zones and climate dates, and accepted-values
tests on payment types. These run as part of `dbt build`, so bad data fails the build.

### 4. Analytics — Looker Studio

A **6-page Looker Studio dashboard** is built on `fact_trips`, connecting through the
native BigQuery connector:

1. **Revenue overview** — total revenue, average fare, revenue per mile, monthly trends,
   revenue by borough.
2. **Demand heatmap** — hourly demand by weekday × hour, top pickup zones, daily volume.
3. **Weather impact** — demand and fare shifts by weather condition, speed degradation.
4. **Route & corridor deep-dive** — airport revenue share, top origin-destination corridors,
   borough-to-borough flow matrix.
5. **Customer behavior** — tip conversion and tip-rate patterns by hour and borough.
6. **Fare-prediction feature explorer** — feature correlations and distributions that feed
   the ML work.

The dashboard layer carries its own engineering: **service-specific p99 outlier caps**
(Yellow fares capped at $52 / 18.76 mi; Green at $46 / 14.19 mi), a library of calculated
fields (`revenue_per_mile`, `trip_duration_min`, `weather_condition`, `temp_band`, sortable
weekday labels), and a consistent dark design system (Inter / DM Mono / Syne typography on a
`#080C18` canvas).

### 5. Machine Learning — PySpark / Spark MLlib

The ML layer reads `dbt_prod.fact_trips` through a **hybrid BigQuery connector**: it uses
the native Spark-BigQuery connector for simple tables, and falls back to the BigQuery Python
client → pandas → Spark path for the wide fact table whose schema the direct connector
handles awkwardly. This is a pragmatic engineering decision — pick the loading strategy per
table rather than forcing one path to handle everything.

Feature engineering spans four families: **temporal** (hour of day, day of week, month),
**spatial** (pickup/dropoff zones), **contextual** (distance, passenger count, service type),
and **weather** (humidity, visibility, temperature, precipitation). The target is
`trip_duration_min`. The train/test split is **chronological** rather than random —
training on earlier months and testing on later ones — which prevents data leakage and
mirrors the real prediction scenario (you predict the future, not a shuffled past). The
regression model is evaluated with RMSE and R².

---

## Key Design Decisions

| Decision | Rationale |
| --- | --- |
| **External tables, not native loads** | Query lake files in place — no data duplication, minimal warehouse storage cost, single source of truth in GCS. |
| **Isolated dbt virtualenv inside the Airflow image** (`/opt/dbt-venv`) | dbt's dependencies never collide with Airflow's pinned set; the DAG calls the venv's dbt binary directly. |
| **`TriggerDagRunOperator` for cross-DAG chaining** | Keeps ingest and table-registration as independent, reusable DAGs while still linking them into one flow. |
| **Seed for zone data, not an external table** | The lookup is small and static; a dbt seed is simpler to version and test than maintaining a separate GCS file + external table. |
| **Surrogate keys to union two services** | Yellow and green taxis have different source schemas; a generated `tripid` lets them merge into one fact table cleanly. |
| **Chronological train/test split** | Prevents leakage and reflects production reality — the model must generalize forward in time. |
| **Environment variables as the single source of truth** | `GCP_PROJECT_ID` / `GCP_GCS_BUCKET` live in one place (Docker Compose) and flow to every DAG; the project is portable with zero code edits. |
| **Three dbt targets (dev / ci / prod)** | CI builds into an isolated `dbt_ci` dataset and **never touches production**; Airflow owns the prod build. |

---

## Engineering Practices & Rigor

**Orchestration infrastructure.** Airflow runs on a **CeleryExecutor** with a Redis broker
and a PostgreSQL metadata database, fully Dockerized via Docker Compose (webserver,
scheduler, worker, triggerer, init, plus Redis and Postgres) with health checks on every
service. A custom Dockerfile layers in the Google providers, PyArrow, and the isolated dbt
venv.

**Testing.** A **59-test pytest suite** gates the work, split into tiers:
- *Unit tests* (no credentials needed) verify every one of the 9 DAG files exists and parses
  as valid Python, that there are no stale project/bucket IDs, that DAGs read config from the
  environment, and that ingest DAGs correctly chain to their external-table DAGs.
- *Integration tests* hit the live cloud — GCS file counts per prefix, BigQuery dataset and
  external-table existence, populated `dbt_prod` tables, and a `dbt compile` that must
  return exit code 0.
- The `conftest.py` **auto-skips integration tests when credentials are absent**, so the
  full suite runs cleanly on any machine or CI runner without secrets.

**CI/CD.** A GitHub Actions workflow validates dbt on every change: `dbt compile` on pull
requests (fast SQL/reference checking, no warehouse writes) and `dbt build` into the
isolated `dbt_ci` dataset on merges to `main`. The service-account key is injected from a
repository secret — never committed.

**Infrastructure-as-code.** Terraform (google provider 5.6.0) declares the GCP resources but
is kept deliberately as reference, with an explicit *"import before apply"* discipline so a
stray `terraform apply` can never clobber the live, hand-verified bucket and datasets.

**Repository hygiene.** The dbt project is a git submodule (remote stays authoritative). The
whole effort is structured as a **phased development plan (Phases 0–5)** where each phase is
isolated, reversible, ends at a test gate, and gets its own commit — so nothing risky and
nothing untested ever lands.

---

## Challenges Solved

**Secret-leak remediation.** A GCP service-account key and an entire vendored
`google-cloud-sdk/` directory had been accidentally committed. Both were purged from the
*full git history* using `git filter-repo`, shrinking the repository from **11 GB to 82 MB**
and hardening `.gitignore` to prevent recurrence — a real-world security and repo-hygiene fix,
not just a `.gitignore` afterthought.

**Reconciling code with live infrastructure.** Rather than trusting that the committed code
matched reality, the live GCP project was treated as the source of truth and the code was
audited against it. This surfaced and fixed roughly **nine structural disconnects** —
stale project/bucket IDs, a missing green-taxi ingest DAG, a missing zone DAG, a climate
file-path bug, and a Dockerfile that tried to install a *macOS* gcloud tarball into a *Linux*
image (which would never have built). After the reconciliation, the pipeline actually runs
end to end.

**Data-quality anomalies.** Several were identified, quantified, and handled deliberately:
service-specific **p99 caps** neutralize fare/distance outliers in the dashboard; an
`Unknown`-borough filter drops ~2.1M trips (1.6% of staged records) *by design*; and a
documented cross-service surrogate-key collision is handled with a stated workaround
(use record counts rather than distinct-key counts where it matters).

---

## Results

The pipeline's validated, locked baseline metrics:

| Metric | Green | Yellow |
| --- | --- | --- |
| Total trips | 28,424,373 | 100,357,273 |
| Total revenue | ~$416M | ~$1.62B |
| Average fare | $12.15 | $12.97 |
| Climate join match | 100% | 100% |
| Zone join drop rate | 0% | 0% |
| Months covered | 24 (2015-01 → 2016-12) | 24 (2015-01 → 2016-12) |

- **~128.8M trips** and **~$2.04B** in fare revenue modeled across both services.
- **100% climate-join match** and **0% zone-join drop** — every staged trip resolves to a
  valid zone and (where applicable) a weather record.
- **~98.4% of staged records retained** in `fact_trips`; the ~1.6% removed are the
  intentional `Unknown`-borough exclusions.
- The entire pipeline is reproducible from a clean checkout: `docker compose up`, trigger
  the DAGs in order, and the modeled tables rebuild from raw source.

---

## Skills Demonstrated

- **Data modeling** — dimensional (fact/dim) modeling, staging-vs-marts ELT, surrogate keys.
- **Orchestration** — Airflow DAG authoring, scheduling, backfills, cross-DAG triggering.
- **Cloud data warehouse** — BigQuery external tables, datasets, cost-aware design.
- **Analytics engineering** — dbt Core, schema tests, macros, seeds, multi-target profiles.
- **Machine learning** — PySpark/MLlib feature engineering, leakage-aware evaluation.
- **Infrastructure-as-code** — Terraform with safe import-before-apply discipline.
- **CI/CD & testing** — GitHub Actions, a 59-test pytest suite, credential-aware skips.
- **Containerization** — multi-service Docker Compose, custom images, dependency isolation.
- **Security & repo hygiene** — secret purging with `git filter-repo`, env-var config, phased delivery.

---

## Roadmap

The platform is built to extend without re-architecting:

- **Productionize the model** — wrap the Spark ML job in a scheduled Airflow batch-prediction
  DAG writing predictions back to BigQuery.
- **Incremental materialization** — convert `fact_trips` to an incremental model partitioned
  by pickup date to cut rebuild cost as data grows.
- **Model registry & serving** — version models and expose a prediction endpoint.
- **Terraform state reconciliation** — import the live resources so infra is fully IaC-managed.
- **OSRM route enrichment** — precompute a 263×263 zone-centroid distance/duration matrix from
  the OSRM routing API, store it in BigQuery, and join it into `fact_trips` to add
  road-network distance and duration as ML features.
- **Extended date ranges** — the ingest DAGs accept additional TLC files with no schema
  changes; adding 2017+ data is a parameter update.

---

*Built end-to-end against a live Google Cloud project, with every quantitative claim above
traceable to the pipeline's validated baseline.*
