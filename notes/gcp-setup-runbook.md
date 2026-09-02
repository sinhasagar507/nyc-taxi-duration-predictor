# GCP Project Setup Runbook

This runbook is the **only part that requires your Google login and billing** — everything
else an agent can do for you. Run it when you want to point the pipeline at a live GCP
project.

**You do not need this to work on the model.** The dbt-built marts are held locally in
`../nyc_taxi_migration_backup/` (dim tables, view SQL, all schemas), which is what lets
`spark/ml/` run end to end with no cloud at all. This runbook is for the *pipeline* half —
Airflow ingest, GCS, BigQuery external tables, and dbt.

---

## Before you start

Pick a project ID: globally unique, lowercase, 6–30 characters. The commands below use
`NEW_PROJECT_ID` as a placeholder — replace it everywhere.

Check the current free-trial terms before you rely on them. Google changes the credit
amount and duration, and the Always Free tier is not the same thing as the trial credit:
<https://cloud.google.com/free>

---

## What you do (about 10 minutes). Run these in your terminal.

> Tip: prefix a command with `! ` in the Claude prompt to run it inline, so the output
> lands in the chat.
>
> `gcloud` must be on your PATH. The repository no longer vendors the SDK. Install it from
> <https://cloud.google.com/sdk/docs/install> if you do not have it.

```bash
# 0. Log in as the Google account that carries the billing you want to use
gcloud auth login

# 1. Create the project
gcloud projects create NEW_PROJECT_ID --name="NYC Taxi Fare"

# 2. Find your billing account, then link it
gcloud billing accounts list                     # copy the ACCOUNT_ID (XXXXXX-XXXXXX-XXXXXX)
gcloud billing projects link NEW_PROJECT_ID --billing-account=ACCOUNT_ID

# 3. Enable the APIs the pipeline needs
gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com \
    storage.googleapis.com --project=NEW_PROJECT_ID

# 4. Create the service account
gcloud iam service-accounts create nytaxi-pipeline \
    --display-name="nytaxi-pipeline" --project=NEW_PROJECT_ID

# 5. Grant it BigQuery and Storage admin. Broad, but proportionate for a learning project.
SA="nytaxi-pipeline@NEW_PROJECT_ID.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding NEW_PROJECT_ID \
    --member="serviceAccount:$SA" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding NEW_PROJECT_ID \
    --member="serviceAccount:$SA" --role="roles/storage.admin"

# 6. Download the key to the STABLE decoupled filename. This is the future-proofing:
#    every consumer reads this one path, so swapping projects never touches code.
gcloud iam service-accounts keys create secrets/gcp-credentials.json --iam-account="$SA"
```

`secrets/` is gitignored. The key must never enter git or a Docker image. On a VM, use an
attached service account (ADC) instead of a key file at all.

---

## Then tell Claude

1. The **project ID** you chose.
2. Confirmation that `secrets/gcp-credentials.json` exists (`ls -la secrets/`).

## What happens next

1. Set `GCP_PROJECT_ID` and `GCP_GCS_BUCKET`. These two env vars in
   `airflow/docker-compose.yaml` are the single source of truth — the DAGs and tests read
   them via `os.environ`, so nothing else needs editing.
2. Create the GCS bucket and the BigQuery datasets. Terraform describes them, but **its
   state is empty**: run `terraform import` before any `terraform apply`, or the apply will
   try to recreate resources and conflict.
3. Run the ingest DAGs to land the raw parquet in GCS, then the external-table DAGs.
4. Run `dbt build` to construct the marts.
5. Run `pytest tests/` — the integration tier stops skipping once credentials resolve.

**One known snag.** The dbt project is a git submodule at `dbt/ny_taxi_analytics`.
`schema_taxi.yml` and `schema_climate.yml` hardcode a project ID as their source database.
Fix that in the upstream repository (github.com/sinhasagar507/ny_taxi_analytics), then bump
the submodule pointer here. Do not edit inside the submodule directory.

Layout reference for buckets and datasets: `notes/gcp-reference.md`.
