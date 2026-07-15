# CLAUDE.md

Guidance for AI agents (and humans) working in this repository.

## Project: NYC Taxi Duration Prediction

End-to-end data engineering pipeline (DataTalksClub DE Zoomcamp) that predicts NYC
taxi trip duration. Intended flow:

```
TLC source data → GCS (raw) → BigQuery external tables → dbt (ELT)
   → fact/dim tables → PySpark duration model + Looker Studio analytics
```

## GCP Infrastructure — reference only (account retired, working locally)

> The GCP project this pipeline originally ran against has been **retired** (trial
> exhausted / account disabled). **Do not treat any GCP resource below as live**, and
> do not expect credentials to be present. Work is **local for now**; the marts were
> backed up locally and to GCS before shutdown. The identifiers (project ID, service
> account, keyfile) have been removed on purpose — supply your own via
> `GOOGLE_APPLICATION_CREDENTIALS` if/when you reconnect to a live project. The layout
> below is retained only as a structural reference for the data model.

- **GCS bucket layout (reference):** `primary-data-dtc` (location US)
  - `nyc_taxi_data/yellow_taxi_data/` — 24 parquet files (2015-01 … 2016-12)
  - `nyc_taxi_data/green_taxi_data/`  — 24 parquet files (2015-01 … 2016-12)
  - `nyc_taxi_data/taxi_lookup_data/taxi_zone_lookup.csv`
  - `nyc_climate_data/climate_data.parquet`
- **BigQuery datasets (reference):**
  - `nyc_taxi_data` — `yellow_taxi_external_table`, `green_taxi_external_table` (EXTERNAL).
    `taxi_zone_external_table` was never created (CSV is in GCS, no table over it).
  - `nyc_climate_data` — `climate_external_table` (EXTERNAL)
  - `dbt_prod` — prod dbt target: `fact_trips`, `dim_zones`,
    `dim_monthly_zones_revenue`, `taxi_zone_lookup`, + staging views
  - `dbt_ssinha` — dev dbt target (mirror of prod)

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

## Environments — venv vs Docker

Rule of thumb: **runs unattended on a schedule → Docker; human-in-the-loop → host `.venv`.**

- **Host `.venv` (repo root, Python 3.13):** pytest, dbt CLI iteration, PySpark/ML
  notebooks, ad-hoc scripts. Invoke it explicitly (`.venv/bin/pytest`, `.venv/bin/python`,
  `.venv/bin/dbt`) — never rely on shell activation, never use system python/pip, and
  don't create additional venvs. New Python deps for tests/ML/dbt go here (and into the
  relevant requirements file). This tooling never deploys.
- **Docker = Airflow only** (`airflow/docker-compose.yaml` + `dockerfile`). Anything a
  DAG imports at runtime goes in the Airflow image, never on the host. The image builds
  its own isolated dbt venv at `/opt/dbt-venv` — that is not the host `.venv`.
- This repo **intentionally overrides the global Docker-first policy** for the
  test/ML/dbt developer workflows; do not containerize them.

### Deployment split (target: GCP, single GCE VM running the compose stack)

- **Deploys:** Airflow image + DAGs (ingest → external tables → `dbt_build_marts`),
  dbt project via the submodule pin, Terraform-managed infra (bucket, BQ datasets).
- **Stays local:** notebooks, EDA artifacts, `spark/head.csv`, `migration_backup/`,
  ML experiments. Tests run locally + in CI, not on the VM.
- **Auth:** keyfile via `GOOGLE_APPLICATION_CREDENTIALS` locally/CI; on the VM use an
  attached service account (ADC) — no keyfiles inside images or git, ever.
- **Prod images are built on the VM itself** (decided 2026-07-10): clone the repo on the
  VM and `docker compose up --build` there. This sidesteps the Apple-Silicon/amd64
  mismatch entirely — local Mac builds are dev-only and must never be pushed to the VM.

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

# dbt compile (supply your own keyfile if reconnecting to a live GCP project)
GOOGLE_APPLICATION_CREDENTIALS=secrets/<your-keyfile>.json \
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

- **✅ Phase 2 — Prune redundancy (isolated, reversible).** Removed truly-unreferenced
  learning-exercise artifacts (legacy BQ homework, experimental notebooks,
  `docker_nana_tutorial`). *Verified:* `pytest tests/` green.

- **✅ Phase 3 — Restructure (mechanical moves only).** Renamed `05_batch_processing/` →
  `spark/` and moved files into the target layout; import paths, Docker volume mounts, and
  dbt project paths updated. No behavioral changes. *Verified:* `pytest tests/` green.

- **✅ Phase 4 — Refactor & harden (behavioral).** DRY'd config to a single source
  (`GCP_PROJECT_ID` / `GCP_GCS_BUCKET`); decoupled credentials from the project-specific
  keyfile name (stable path `secrets/gcp-credentials.json`, guard test); parametrized the
  ingest window via `INGEST_START_DATE` / `INGEST_END_DATE`. *Verified:* `pytest tests/` green.

- **⏳ Phase 5 — Document (in progress).** Reconcile this file + README to the final
  structure and retired-account reality. Open PR.

## Commit Conventions

- **Never add `Co-Authored-By` trailers to commit messages.** The repository owner does
  not want AI model names to appear as contributors or co-authors on GitHub. Commit solely
  under the git user identity configured in the repo (`Sagar Sinha`). This applies to all
  commits on all branches, including automated or agent-driven commits.

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
  `GOOGLE_APPLICATION_CREDENTIALS=secrets/<your-keyfile>.json dbt build --project-dir dbt/ny_taxi_analytics --profiles-dir dbt`.
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
