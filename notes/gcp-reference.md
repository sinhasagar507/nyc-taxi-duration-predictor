# GCP Infrastructure — reference layout

**Invoke when:** you need the bucket or dataset layout, the test layout, or the E2E smoke steps.

Moved out of CLAUDE.md 2026-08-22. **No GCP project is provisioned.** Do not treat any
resource below as live, and do not expect credentials to be present.

The dbt-built marts are held locally, **outside the repository** since audit item 10
(2026-09-01), in the sibling `nyc_taxi_migration_backup/`; `spark/ml/src/paths.py`
resolves that path and `MIGRATION_BACKUP_DIR` overrides it. That local copy is what lets
the fare model run with no cloud.

Identifiers (project ID, service account, keyfile) are left out on purpose — supply your
own via `GOOGLE_APPLICATION_CREDENTIALS` plus `GCP_PROJECT_ID` / `GCP_GCS_BUCKET`. The
layout below is retained as a structural reference for the data model.

## GCS bucket layout

Bucket `primary-data-dtc` (location US):

- `nyc_taxi_data/yellow_taxi_data/` — 24 parquet files (2015-01 … 2016-12)
- `nyc_taxi_data/green_taxi_data/`  — 24 parquet files (2015-01 … 2016-12)
- `nyc_taxi_data/taxi_lookup_data/taxi_zone_lookup.csv`
- `nyc_climate_data/climate_data.parquet`

## BigQuery datasets

- `nyc_taxi_data` — `yellow_taxi_external_table`, `green_taxi_external_table` (EXTERNAL).
  `taxi_zone_external_table` was never created (CSV is in GCS, no table over it).
- `nyc_climate_data` — `climate_external_table` (EXTERNAL)
- `dbt_prod` — prod dbt target: `fact_trips`, `dim_zones`, `dim_monthly_zones_revenue`,
  `taxi_zone_lookup`, + staging views
- `dbt_ssinha` — dev dbt target (mirror of prod)

## Test structure (moved from CLAUDE.md Testing section)

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

(`tests/unit/ml/` has since been added for the fare-model modules; run `ls tests/` for
the current truth.)

## E2E smoke test (manual)

```bash
docker compose -f airflow/docker-compose.yaml up --build -d
# → open localhost:8080, trigger nyc_taxi_gcs_yellow_dag, watch task states
```

## Development plan history (Phases 0–4, full text)

Compressed in CLAUDE.md 2026-08-22; original detail:

- **Phase 0 — Checkpoint.** Branch off `main`; committed current state. `.gitignore`
  covers `secrets/`, `.venv/`, `google-cloud-sdk/`, local data dumps.
- **Phase 1 — Reconcile config with live infra.** All stale project/bucket IDs fixed.
  Green ingest DAG created; climate path bug fixed; zone ingest DAG created; all 4
  external-table DAGs migrated to env vars; ingest DAGs chained to external-table DAGs
  via `TriggerDagRunOperator`; dbt submodule + dbt Core Airflow DAG + CI/CD wired;
  PySpark pointed at `dbt_prod`. Verified: `pytest tests/` green; `dbt compile` clean;
  `docker compose up --build` succeeds; all DAGs load in Airflow UI without import errors.
- **Phase 2 — Prune redundancy.** Removed truly-unreferenced learning-exercise artifacts
  (legacy BQ homework, experimental notebooks, `docker_nana_tutorial`).
- **Phase 3 — Restructure (mechanical moves only).** `05_batch_processing/` → `spark/`;
  import paths, Docker volume mounts, dbt project paths updated. No behavioral changes.
- **Phase 4 — Refactor & harden.** DRY'd config to `GCP_PROJECT_ID` / `GCP_GCS_BUCKET`;
  stable credentials path `secrets/gcp-credentials.json` + guard test; parametrized the
  ingest window via `INGEST_START_DATE` / `INGEST_END_DATE`.
