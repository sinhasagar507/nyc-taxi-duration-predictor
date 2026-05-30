# CLAUDE.md

Guidance for AI agents (and humans) working in this repository.

## Project: NYC Taxi Duration Prediction

End-to-end data engineering pipeline (DataTalksClub DE Zoomcamp) that predicts NYC
taxi trip duration. Intended flow:

```
TLC source data → GCS (raw) → BigQuery external tables → dbt (ELT)
   → fact/dim tables → PySpark duration model + Looker Studio analytics
```

## Live GCP Infrastructure — SOURCE OF TRUTH

Verified directly via `gcloud`/client libraries against the live project. When code
and this table disagree, **the table is correct and the code must be reconciled to it.**

- **Project ID:** `dtc-de-project-492321`
- **Service account:** `dtc-de-course@dtc-de-project-492321.iam.gserviceaccount.com`
- **Credentials file:** `secrets/dtc-de-project-492321-970e67a252d8.json`
- **GCS bucket:** `primary-data-dtc` (location US)
  - `nyc_taxi_data/yellow_taxi_data/` — 24 parquet files (2015-01 … 2016-12)
  - `nyc_taxi_data/green_taxi_data/`  — 24 parquet files (2015-01 … 2016-12)
  - `nyc_taxi_data/taxi_lookup_data/taxi_zone_lookup.csv`
  - `nyc_climate_data/climate_data.parquet`
- **BigQuery datasets:**
  - `nyc_taxi_data` — `yellow_taxi_external_table`, `green_taxi_external_table` (EXTERNAL).
    ⚠️ `taxi_zone_external_table` is **not yet created** (CSV is in GCS, no table over it).
  - `nyc_climate_data` — `climate_external_table` (EXTERNAL)
  - `dbt_prod` — **prod** dbt target: `fact_trips`, `dim_zones`,
    `dim_monthly_zones_revenue`, `taxi_zone_lookup`, + staging views
  - `dbt_ssinha` — **dev** dbt target (mirror of prod)

- dbt project is vendored as a **git submodule** at `dbt/ny_taxi_analytics`
  (remote: github.com/sinhasagar507/ny_taxi_analytics; remote stays authoritative — the
  submodule just pins a commit). It runs via **dbt Core** locally: Airflow's
  `dbt_build_marts` DAG runs `dbt build` from an isolated venv, and GitHub Actions
  (`.github/workflows/dbt.yml`) validates on PR / builds on main. Verified stack:
  dbt-core 1.11 + dbt-bigquery 1.11 on Python 3.12.

## Target Directory Structure (refactor goal)

```
nyc_taxi_durationprediction/
├── terraform/        # infra (reconcile vars with live resources; import before apply)
├── airflow/          # orchestration: dags/, docker-compose.yaml, dockerfile
├── dbt/
│   ├── ny_taxi_analytics/   # git submodule → github.com/sinhasagar507/ny_taxi_analytics
│   ├── profiles.yml         # BigQuery profile (env-var keyfile; dev/ci/prod targets)
│   └── requirements.txt     # dbt-bigquery==1.11.1 (local + CI + Airflow image)
├── .github/workflows/dbt.yml  # CI/CD: dbt compile on PR, dbt build on main
├── spark/            # batch processing + ML notebooks (was 05_batch_processing/)
├── bigquery/         # SQL reference queries
├── tests/            # project-wide test suite (see ## Testing below)
├── secrets/          # service-account keys (gitignored)
└── notes/            # course notes / documentation
```

**Artifacts to retire** (learning leftovers, not pipeline components):
`03_data_warehouse_bigquery/`, `04_analytics_engineering/`, `docker_nana_tutorial/`,
`gcs_storage/`, `spark_data/`, `google-cloud-sdk/` (vendored SDK), `project_architecture/`,
and `bigquery/queries/*.sql` (legacy homework against the dead `dtc-de-course-440404.nytaxi`
project — FHV/2023-24/BQML scratch, not part of the current 2015-16 duration pipeline).

## Testing

Each development phase must pass its verification gate before the next phase begins.
Run `pytest tests/` from the repo root. Unit tests run without credentials; integration
tests auto-skip if credentials are unavailable.

### Tiers

| Tier | Requires | Run command | Gate for |
| --- | --- | --- | --- |
| **Unit** | nothing | `pytest tests/unit/` | every commit |
| **Integration** | `GOOGLE_APPLICATION_CREDENTIALS` | `pytest tests/integration/` | Phase 1, Phase 3, Phase 4 |
| **dbt** | credentials + dbt Core | `dbt compile --project-dir dbt/ny_taxi_analytics --profiles-dir dbt` | Phase 1 |
| **E2E** | `docker compose up` | trigger DAGs in Airflow UI, check task states | Phase 1, Phase 4 |

### Structure

```
tests/
├── conftest.py                     # constants, auto-skip integration without creds
├── unit/
│   └── dags/
│       ├── test_dag_integrity.py   # all 9 DAG files exist + parse as valid Python
│       └── test_dag_config.py      # no stale IDs, env var usage, DAG chaining
└── integration/
    ├── test_gcs.py                 # bucket reachable, file counts per prefix
    ├── test_bigquery.py            # datasets + external tables exist + queryable
    └── test_dbt.py                 # dbt compile returns 0
```

### Quick-run reference

```bash
# Unit only (no credentials needed)
pytest tests/unit/ -v

# Integration (reads creds from secrets/ automatically)
pytest tests/integration/ -v

# Everything — integration auto-skips gracefully if no creds
pytest tests/ -v

# dbt compile
GOOGLE_APPLICATION_CREDENTIALS=secrets/dtc-de-project-492321-970e67a252d8.json \
  dbt compile --project-dir dbt/ny_taxi_analytics --profiles-dir dbt

# E2E smoke test (manual)
docker compose -f airflow/docker-compose.yaml up --build -d
# → open localhost:8080, trigger nyc_taxi_gcs_yellow_dag, watch task states
```

Note: `airflow/tests/` contains legacy TDD stubs that require Airflow installed locally
(not the Docker image). They are not part of the standard test run; treat them as
documentation of expected DAG behaviour until they are migrated.

## Development Plan — sequenced for safety

Each phase ends with a test-suite verification gate and its own commit. Do not mix phases.

- **✅ Phase 0 — Checkpoint.** Branch off `main`; committed current state. `.gitignore`
  covers `secrets/`, `.venv/`, `google-cloud-sdk/`, local data dumps.

- **✅ Phase 1 — Reconcile config with live infra.** All stale project/bucket IDs fixed.
  Green ingest DAG created; climate path bug fixed; zone ingest DAG created; all 4
  external-table DAGs migrated to env vars; ingest DAGs chained to external-table DAGs
  via `TriggerDagRunOperator`; dbt submodule + dbt Core Airflow DAG + CI/CD wired;
  PySpark pointed at `dbt_prod`.
  *Verify:* `pytest tests/` green; `dbt compile` clean; `docker compose up --build`
  succeeds; all DAGs load in Airflow UI without import errors.

- **Phase 2 — Prune redundancy (isolated, reversible).** Remove only truly-unreferenced
  artifacts (grep for imports/paths first). Nothing else changes in this commit.
  *Verify:* `pytest tests/` still green.

- **Phase 3 — Restructure (mechanical moves only).** Move files into the target layout;
  update import paths, Docker volume mounts, and dbt project paths. No behavioral
  changes. *Verify:* `pytest tests/` green. Commit.

- **Phase 4 — Refactor & harden (behavioral).** DRY the config (single source for
  project/bucket), parametrize date ranges. Each change isolated + verified. Commit.

- **Phase 5 — Document.** Update this file + README to match the final structure. Open PR.

## Conventions & Gotchas
- **Single source of truth** for project/bucket = Airflow `docker-compose.yaml` env vars
  (`GCP_PROJECT_ID`, `GCP_GCS_BUCKET`); DAGs read them via `os.environ`. All ingest *and*
  external-table DAGs now follow this — keep new DAGs on env vars too.
- dbt prod dataset is **`dbt_prod`**, not `dbt_production`. Keep PySpark in sync.
- dbt runs via **dbt Core** locally (no dbt Cloud). Three execution contexts share one
  `dbt/profiles.yml` (profile `default`, keyed off `GOOGLE_APPLICATION_CREDENTIALS` so no
  secret is committed): local CLI, Airflow (`dbt_build_marts` DAG → isolated
  `/opt/dbt-venv` built in the dockerfile, target `prod`), and CI (`.github/workflows/dbt.yml`,
  target `ci` → `dbt_ci` dataset). Local run:
  `GOOGLE_APPLICATION_CREDENTIALS=secrets/…492321….json dbt build --project-dir dbt/ny_taxi_analytics --profiles-dir dbt`.
  CI needs the `GCP_SA_KEY` repo secret. dbt artifacts (`target/`, `dbt_packages/`) land
  inside the submodule and are gitignored there. Edit dbt models in the **remote repo**,
  then bump the submodule pointer — don't edit inside the submodule here.
- Taxi-zone data reaches dbt via a **seed** (`seeds/taxi_zone_lookup.csv` →
  `ref('taxi_zone_lookup')`), NOT `taxi_zone_external_table`. The zone ingest +
  external-table DAGs are a parallel path dbt does not depend on.
- **Never `terraform apply` against the live resources without `terraform import` first.**
  TF state is empty, so an apply would try to recreate the existing bucket/datasets and
  conflict. Treat Terraform as reference until state is reconciled.
- The Google Cloud SDK is vendored at `google-cloud-sdk/`; `gsutil` needs Python ≤3.12,
  but the BigQuery/Storage **client libraries** work fine under the repo `.venv` (3.13).
- The Airflow **dockerfile no longer installs gcloud** — the old step pulled a *macOS*
  tarball (`…darwin-arm.tar.gz`) into a Linux image (would fail to build) and no DAG uses
  the gcloud CLI (they use the Python client libs + Airflow operators).
