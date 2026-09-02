# GCP Account Migration Runbook

Old project `dtc-de-project-492321` free trial exhausted. This runbook is the **only part
that requires your Google login/billing** — everything else Claude does for you.

Backup already taken → `../nyc_taxi_migration_backup/` (dim tables, view SQL, all
schemas). It sat inside the repo until audit item 10 moved it out on 2026-09-01. Old BQ is
still readable as of migration start, so the marts can be copied directly once the new
project exists.

---

## What you do (≈10 min). Run these in your terminal.

> Tip: prefix with `! ` in the Claude prompt to run inline so output lands in the chat.
> If `gcloud` isn't on PATH, use the vendored one: `./google-cloud-sdk/bin/gcloud`.

Pick a new project ID (globally unique, lowercase, 6–30 chars). Example below uses
`NEW_PROJECT_ID` — replace it everywhere. Suggestion: `dtc-de-nytaxi-<something>`.

```bash
# 0. Log in as the Google account that has the fresh trial / paid billing
gcloud auth login

# 1. Create the project
gcloud projects create NEW_PROJECT_ID --name="NYC Taxi Duration"

# 2. Find your billing account, then link it (enables the new $300 trial credit)
gcloud billing accounts list                     # copy the ACCOUNT_ID (XXXXXX-XXXXXX-XXXXXX)
gcloud billing projects link NEW_PROJECT_ID --billing-account=ACCOUNT_ID

# 3. Enable the APIs the pipeline needs
gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com \
    storage.googleapis.com --project=NEW_PROJECT_ID

# 4. Create the service account (same name as before, new project)
gcloud iam service-accounts create dtc-de-course \
    --display-name="dtc-de-course" --project=NEW_PROJECT_ID

# 5. Grant it admin on BigQuery + Storage in the NEW project (fine for a learning project)
SA="dtc-de-course@NEW_PROJECT_ID.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding NEW_PROJECT_ID \
    --member="serviceAccount:$SA" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding NEW_PROJECT_ID \
    --member="serviceAccount:$SA" --role="roles/storage.admin"

# 6. Download the key to the STABLE decoupled filename (this is the future-proofing)
gcloud iam service-accounts keys create secrets/gcp-credentials.json --iam-account="$SA"

# 7. Let the NEW service account READ the OLD project's BigQuery so Claude can copy the marts
gcloud projects add-iam-policy-binding dtc-de-project-492321 \
    --member="serviceAccount:$SA" --role="roles/bigquery.dataViewer"
```

If IAM on the old project (step 7) is blocked because billing is disabled, tell Claude —
we'll copy `fact_trips` authenticated as your own user identity instead.

---

## Then paste back to Claude:

1. The **new project ID** you chose.
2. Confirm `secrets/gcp-credentials.json` exists (`ls -la secrets/`).

Claude then: creates `dbt_prod` in the new project → copies the 4 mart tables (incl. the
59 GB `fact_trips`) → recreates the 4 views → repoints all code to the new project with the
decoupled credential path → runs the tests. The raw-data / bucket / dbt-from-scratch rebuild
is deferred to the end (your call).
