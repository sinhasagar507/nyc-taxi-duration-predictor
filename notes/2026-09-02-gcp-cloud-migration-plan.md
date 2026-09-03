# GCP Cloud Migration Plan

**Invoke when:** any cloud, Terraform, Dataproc, BigQuery or cost question, and before spending anything.

**Created:** 2026-09-02
**Author:** Sagar Sinha (with Claude Code)
**Answers:** audit item 5 (`notes/2026-08-22-repo-audit.md`) — "record a decision: provision a
project, build a local substitute, or archive-as-reference." Also carries items 6 and 8 and
resolves D-006.
**Companion docs:** `spark/2026-07-10-fare-prediction-modeling-plan.md` (§5b, §5c, §10),
`notes/gcp-setup-runbook.md`, `notes/gcp-reference.md`, `notes/decisions.md`.

**Decision, stated first: provision.** Stand the pipeline up on Google Cloud under
`$GCP_PROJECT_ID` (default `dtc-de-project-506916`), on the free-trial credit, with every
billable resource either serverless or deleted when idle. Not a local substitute (the
pipeline's GCS → BigQuery external-table → dbt path is the portfolio artefact, and a
substitute proves nothing about it) and not archive-as-reference (the §5c full-scale run has
no other target).

---

## 0. Framing

**What this plan is for.** Two things that both need a live project: proving the Airflow /
dbt / Terraform half of the pipeline, which has never run against real infrastructure, and
running modeling-plan §5c (full-scale training on the 12,748,027-row `sample_full`). It also
answers the owner's requirement that modeling move to PySpark — §5 argues that honestly.

**What it assumes.**

- A $300 Welcome credit, usable over 90 days, attached to the billing account for
  `$GCP_PROJECT_ID`. Verified 2026-09-02 against `docs.cloud.google.com/free/docs/free-cloud-features`.
  Eligibility is the owner's to confirm. If no credit applies, every figure in §3 becomes
  real dollars — and at this project's scale the figures are the same size.
- The 7.1 GB local `fact_trips` backup (128,781,646 rows, 204 parquet files, resolved by
  `spark/ml/src/paths.py`) stays the modeling input until BigQuery reproduces it.
- The target is `fare_capped` (D-001, LOCKED). The folder name says "duration"; it is wrong
  and stays.
- The sealed 153,153-row holdout is scored once, at the end of modeling Phase 5. Nothing in
  this plan touches it.

**What it excludes.** D-002 / D-003 / D-004 (DEFERRED — not raised here). `CASE_STUDY.md`
(D-005 — waits on Phase 5). Modeling Phase 6 (neural nets). Dashboard v3 beyond
reconnecting Looker Studio to the rebuilt `dbt_prod`. The OSRM routing integration is
scoped in §6 but not scheduled.

**Measured vs assumed — the house rule.** This project has twice built on library
behaviour it never checked (`d83141b`, the §5b "MLlib has no TargetEncoder" claim). Every
external figure below carries one of three labels: **VERIFIED** (read from the primary
page on 2026-09-02, with the literal text captured), **CORROBORATED** (consistent across
secondary sources, primary page not captured), or **UNVERIFIED / ASSUMED**. A figure with
no label is measured in this repository.

**A note on rotting links.** Google has folded the Dataproc pricing page into "Managed
Service for Apache Spark (formerly Dataproc)", the Composer page into "Managed Service for
Apache Airflow", and `cloud.google.com/vertex-ai/pricing` now 301-redirects to
`cloud.google.com/products/gemini-enterprise-agent-platform/pricing`. `cloud.google.com/*/docs/*`
redirects to `docs.cloud.google.com`. Expect every URL in this document to move; the
product names above are the search terms that still find them.

---

## 1. Where the repository is today

### 1.1 Works today, no cloud needed

| Component | Evidence |
|---|---|
| `spark/ml/` end to end: `00_prep_spark.py` → `features.py` → `01_run_sweep.py` → `01_mllib_baseline.py` | Phase 4b complete 2026-09-01; results in `spark/ml/results/` |
| Unit suite | `.venv/bin/pytest tests/` on 2026-09-02: **285 passed, 9 failed, 1 skipped**; all 9 failures are `tests/integration/` |
| `dbt compile` against the submodule SQL | needs no warehouse |
| Dev container (`docker/dev/Dockerfile`: Python 3.12, JRE 17, `pyspark==4.1.2`) | builds and runs the suite |

### 1.2 Wired, never proven against live infrastructure

- All 9 DAGs in `airflow/dags/`: four ingest (`nyc_taxi_gcs_{yellow,green}_dag.py`,
  `nyc_climate_gcs_dag.py`, `nyc_taxi_zone_gcs_dag.py`), four `CREATE OR REPLACE EXTERNAL
  TABLE` DAGs, and `dbt_run_dag.py` (`dbt deps && dbt build --target prod` in
  `/opt/dbt-venv`). Every one reads `GCP_PROJECT_ID` / `GCP_GCS_BUCKET` from `os.environ`;
  no DAG hardcodes an ID. Ingest DAGs chain to their external-table DAG via
  `TriggerDagRunOperator`. None has ever been triggered against a bucket that exists.
- `dbt build` / `dbt run` (any target), and the CI `dbt build` in `.github/workflows/dbt.yml`
  (needs the `GCP_SA_KEY` secret, target `ci` → `dbt_ci`).
- `terraform/main.tf`: declares one resource, `google_bigquery_dataset.demo_dataset`
  (default name `nyc_tlc_trips`), provider `hashicorp/google` 5.6.0. That dataset name
  appears nowhere else in the pipeline. The bucket in `variables.tf`
  (`primary-data-dtc-506916`) is not declared at all.
- The GCP half of the README pipeline diagram.

Dead config worth knowing about, not fixing now: `nyc_climate_gcs_dag.py` reads
`BIGQUERY_DATASET` / `CLIMATE_TABLE` and never uses them (its inline external-table block
is commented out); the yellow/green DAGs define `TAXI_BIGQUERY_DATASET_ID` /
`..._TABLE_ID` and never use them.

### 1.3 Does not exist

- A provisioned project answering to `dtc-de-project-506916`. Every file that names it
  names it as a *default* (`dbt/profiles.yml` via `env_var('GCP_PROJECT_ID', ...)`,
  `terraform/variables.tf`, the Airflow compose env). Nothing confirms it is live, and the
  tracked Terraform state points elsewhere (1.4).
- The §5c full-scale run. OSRM route features (README Future Scope, zero code). Any
  Vertex AI reference.
- `taxi_zone_external_table` — never created; dbt gets zones from
  `seeds/taxi_zone_lookup.csv`, not the external table (the zone DAGs are a parallel path).

### 1.4 Three stale project IDs — the cleanup the migration must start with

CLAUDE.md says "TF state is empty". It is not empty; it is foreign.

| Where | What it holds | Project ID it names |
|---|---|---|
| `terraform/terraform.tfstate` (serial 3, lineage `c1b3ce47…`) | `google_bigquery_dataset.demo_dataset` = `projects/…/datasets/nyc_tlc_trips` | `dtc-de-course-457315` |
| `terraform/terraform.tfstate.1753224766.backup` (serial 1, lineage `16740d2c…`) | `google_storage_bucket.demo-bucket` = `dtc-de-project_1` | `dtc-de-course-466501` |
| `terraform/terraform.tfstate.backup` (serial 2, lineage `16740d2c…`) | nothing | — |
| `dbt/ny_taxi_analytics/models/staging/schema_taxi.yml:5` and `schema_climate.yml:5` | `database:` for the source definitions | `dtc-de-project-492321` |

The live tfstate and the two backups have different lineages — the state was re-initialised
at some point, so the live file is a second, disconnected history. None of the three
resources matches the current `variables.tf` bucket or the datasets the pipeline actually
uses (`nyc_taxi_data`, `nyc_climate_data`, `dbt_prod`). All three tfstate files are
tracked in git, which is itself the defect: state can hold secrets and is never
merge-safe.

The submodule IDs are the runbook's "known snag". They are fixed **upstream**
(`github.com/sinhasagar507/ny_taxi_analytics`) and the pointer bumped; CLAUDE.md forbids
editing inside `dbt/ny_taxi_analytics`. The fix is `database: "{{ env_var('GCP_PROJECT_ID') }}"`
(dbt resolves `env_var` in source YAML), which also makes the `ci` target's project follow
the same variable.

### 1.5 The 9-failure baseline

`tests/conftest.py` picks up `secrets/gcp-credentials.json` if present. On 2026-09-02 a
credential resolved and authenticated, and every integration test then failed on missing
resources (0 of 24 yellow files, 0 of 24 green, no datasets). That is the expected shape of
"no project provisioned", identical before and after the 2026-09-01 session. §4's gates
count down from 9 to 0; a change that moves the number in the other direction is a
regression.

---

## 2. Target architecture on GCP

```
 TLC CDN (d37ci6vzurychx.cloudfront.net)            NOAA/gist CSV       TLC zone CSV
        │ curl (ingest DAGs, INGEST_START_DATE..INGEST_END_DATE)   │                │
        ▼                                                          ▼                ▼
 ┌────────────────────────── GCS  gs://$GCP_GCS_BUCKET  (us-central1) ─────────────────────┐
 │ nyc_taxi_data/yellow_taxi_data/*.parquet   nyc_climate_data/*.parquet   taxi_lookup_data/ │
 │ nyc_taxi_data/green_taxi_data/*.parquet    ml/samples/*.parquet  ml/models/  tfstate?     │
 └───────────────┬───────────────────────────────────────────────┬───────────────────────────┘
                 │ CREATE OR REPLACE EXTERNAL TABLE (4 DAGs)      │ spark-4.0-bigquery 0.45.0 /
                 ▼                                                │ plain parquet read
 ┌──────────── BigQuery ─────────────┐                            ▼
 │ nyc_taxi_data.{yellow,green}_…    │   ┌──────── Dataproc Serverless, runtime 3.0 ───────┐
 │ nyc_climate_data.…                │   │ Spark 4.0.1 · Scala 2.13 · Python 3.12          │
 │        │ dbt build --target prod   │   │ 00_prep_spark.py  ·  01_mllib_baseline.py       │
 │        ▼                          │◄──│ §5c full-scale run; §5 OOF encoder              │
 │ dbt_prod.fact_trips (partition+   │   │ bills per second, scales to zero                 │
 │   cluster), dim_zones, dim_…      │   └─────────────────────────────────────────────────┘
 └───────────────┬───────────────────┘
                 │                          ┌──────── GCE VM (amd64), attached SA (ADC) ────┐
                 ▼                          │ airflow/docker-compose.yaml, images built here │
        Looker Studio (dashboard v3)        │ STOPPED whenever no DAG is scheduled to run    │
                                            └────────────────────────────────────────────────┘
 Terraform: bucket + datasets, state reset, import-before-apply.   Budget alerts: $50 / $150 / $250.
```

### 2.1 Storage — GCS bucket, one, regional `us-central1`

**Why.** The four ingest DAGs and the four external-table DAGs already target
`gs://{GCP_GCS_BUCKET}/nyc_taxi_data/…`, `nyc_climate_data/…`, `taxi_lookup_data/…`
(`notes/gcp-reference.md`). Keeping the layout keeps eight DAGs and three integration
tests unchanged. A US region is the Always Free condition for the 5 GB-month allowance
(VERIFIED). New prefixes for the ML side: `ml/samples/` (uploaded `sample_work_train.parquet`
and `sample_full.parquet`), `ml/models/`.

**Rejected: loading raw parquet straight into native BigQuery tables.** It would drop the
external-table layer the DAGs and dbt sources are built on, and the raw files must land in
GCS anyway for Spark to read them without the BigQuery connector.

**Location question — RESOLVED 2026-09-02, VERIFIED against the BigQuery docs.** A `US`
multi-region dataset **can** define and query an external table over a `us-central1` bucket,
and pays no data-transfer charge for it. `docs.cloud.google.com/bigquery/docs/locations`:
"If your BigQuery dataset is in the `US` multi-region, then the Cloud Storage bucket can be
in the Iowa (`us-central1`) single region, or any dual-region that includes Iowa." The
stricter sentence on the external-table page — "The Cloud Storage bucket must be in the same
location as the dataset that contains the table you're creating" — is the general rule; the
locations page carries the multi-region exception. Any **other** single region, `us-west1`
for example, does incur transfer charges even though it sits inside the US multi-region.

So the two Terraform variables already agree: `var.location = "US"` for the datasets,
`var.region = "us-central1"` for a new bucket. `dbt/profiles.yml` stays on `location: US`.
Nothing has to change together.

**Correction to this section.** It said "`dbt/profiles.yml` sets no `location`". That is false
as measured: all three targets (`dev`, `ci`, `prod`) hardcode `location: US`.

### 2.2 Warehouse — BigQuery, external tables + dbt marts

**Why.** Already built: three staging views and three core models in the submodule;
`dbt_prod` as the prod dataset (never `dbt_production`). BigQuery costs nothing at this
scale (§3), and Looker Studio reads it natively.

**Change to make upstream (in the same submodule PR as the ID fix):** `fact_trips.sql`
gets `{{ config(partition_by={"field": "pickup_datetime", "data_type": "timestamp",
"granularity": "month"}, cluster_by=["pickup_locationid", "dropoff_locationid"]) }}`.
Monthly, not daily: 128.8M rows over 730 days is ~176K rows a day — under BigQuery's own
several-GB-per-partition guidance, so daily partitions would be many and thin. Cluster on
the two zone IDs because `od_corridor` and the dashboard's corridor/borough-flow pages
filter on them; `service_type` has two values and is not worth a slot.

**Read 2026-09-02, as this bullet asked.** `models/core/fact_trips.sql` line 1 is
`{{ config(materialized='table') }}` and nothing else: no `partition_by`, no `cluster_by`,
so the change is additive and overwrites no existing choice. The three columns it names all
exist in the model's own `select` (`tu.pickup_datetime`, `tu.pickup_locationid`,
`tu.dropoff_locationid`), and `pickup_datetime` really is a timestamp — the staging models
build it with `cast(lpep_pickup_datetime as timestamp)`, so `"data_type": "timestamp"` is
right and not assumed. The patch is prepared and dry-run in M0.

**Rejected:** BigQuery Editions / slot reservations (on-demand with a 1 TiB free month is
the right model for a pipeline that builds marts a handful of times).

### 2.3 Orchestration — self-hosted Airflow, compose stack, one GCE VM, stopped when idle

**Why.** `airflow/docker-compose.yaml` + `airflow/dockerfile` (`apache/airflow:2.10.3`,
isolated `/opt/dbt-venv` with `dbt-bigquery==1.11.1`) is built and documented. The
"Deployment split" design (CLAUDE.md, decided 2026-07-10) already says: single VM, images
built on the VM, attached service account, no keyfiles in images or git. This plan keeps
it — which resolves **D-006: keep the subsection.**

**The operational rule that makes it affordable:** the VM is *stopped* (not merely idle)
between runs. A VM bills for every second it exists in the running state, whether or not a
DAG is executing, and the budget is a fixed credit. Persistent disk still bills while
stopped — 30 GB-months of standard PD are Always Free (VERIFIED), so a ≤30 GB boot disk
costs nothing stopped.

**Before the VM exists at all,** the DAGs are proven from the laptop: the same compose
stack runs locally with `GOOGLE_APPLICATION_CREDENTIALS` pointed at
`secrets/gcp-credentials.json` and talks to the live bucket and datasets. That is §4 Phase
M2. It costs zero compute and catches every DAG bug before any VM is paid for. **How the
compose file mounts the keyfile into the containers was not read in the research** — check
`airflow/docker-compose.yaml` for the volume and env line before M2.

**Rejected: Cloud Composer.** It does not scale to zero — Google's own scaling docs say the
scheduler, DAG processor, triggerer and web server run continuously as long as the
environment exists; only workers autoscale, and to a floor you set (VERIFIED). The rate is
$0.06 per 1,000 milliDCU-hours (VERIFIED); the minimum monthly bill for the smallest
persistent environment is **UNVERIFIED** (secondary sources put it at $300–450). Even the
low end would consume the credit in a month for a pipeline that runs a DAG a few times.
Composer 1 and 2.0.x also reach end of life on 2026-09-15 (CORROBORATED).

### 2.4 Spark — Dataproc Serverless, runtime 3.0 (Spark 4.0.1). Accept the minor-version step down.

Three options were on the table: accept Spark 4.0.1 on Dataproc Serverless; build a custom
Dataproc image with 4.1.2; run a plain GCE VM with the local Spark install. Two facts settle
it.

**Fact 1 — the 4.1.2 pin is not load-bearing.** The only Spark-4-specific class the
pipeline uses is `pyspark.ml.feature.TargetEncoder`, added in **4.0.0**. It exists in 4.0.1.
Every other class in `spark/ml/src/mllib.py` and the two scripts — `Pipeline`,
`StringIndexer`, `OneHotEncoder`, `SQLTransformer`, `VectorAssembler`, `GBTRegressor`,
`RegressionEvaluator` — predates Spark 3. `requirements.txt` pins 4.1.2 because it was
current on 2026-07-28, not because anything needs it.

**Fact 2 — the managed runtime is closer to the dev container than the host venv is.**
Runtime 3.0.13 (released 2026-03-27) is Spark 4.0.1, Scala 2.13, Python 3.12 (VERIFIED,
`docs.cloud.google.com/dataproc-serverless/docs/concepts/versions/spark-runtime-3.0`).
`docker/dev/Dockerfile` is Python 3.12 on JRE 17. The host `.venv` is Python 3.13. The
BigQuery connector `com.google.cloud.spark:spark-4.0-bigquery:0.45.0` has been GA since
0.44.0 (2026-02-11) and needs Java 17+ (VERIFIED, connector `CHANGES.md`). `spark-4.1-bigquery`
went GA in 0.45.0 on 2026-08-21 — so a 4.1 path exists too, but nothing here needs it.

**Why not a custom image with 4.1.2.** It buys a version nobody depends on and costs an
image build and a maintenance surface on the Apple-silicon/amd64 boundary CLAUDE.md
already warns about.

**Why not a plain GCE VM for Spark — the decisive argument.** A VM bills while it *exists*,
not while it *works*. The budget is a fixed credit. Serverless bills per second from
submit to finish and then stops (VERIFIED: "scale-to-zero ensures you never pay for idle
capacity"). For one-shot 15–60 minute batch jobs with no persistent cluster to amortise,
that is the whole difference. Classic Dataproc clusters were rejected for the same reason
plus the $0.01/vCPU-hour management fee on top of the VM price (VERIFIED, twice on the page).

**Two things the runtime page says that the plan must respect.** Runtime 3.0 does not
support Lightning Engine / Native Query Execution (irrelevant here), and "end-user
credentials are used for all workloads by default" — the batch's service account and its
roles must be set explicitly. The required role set for a Serverless batch is not in the
research — **UNVERIFIED**; look it up before M4. Whether runtime 3.0 bundles a BigQuery
connector, and which version, is also UNVERIFIED — pass the 0.45.0 coordinate explicitly via
`spark.jars.packages` so the run does not depend on it.

**The cheap de-risking test, before any cloud spend.** Two Spark 4.1.2 facts landed in
`mllib.py` on 2026-09-01 (`targetType` defaults to `"binary"`; `TargetEncoderModel` copies
nominal metadata onto its output, tripping `VectorAssembler` / `maxBins`). Either could
differ in 4.0.1. So: pin `pyspark==4.0.1` in a throwaway dev-container build, re-run the
Phase 4b row 2 configuration (`01_mllib_baseline.py`, 612,608 rows, 5 folds,
`maxIter=100`, `maxDepth=5`, smoothing 5) and compare against the recorded baseline —
**MAE 0.5202 ±0.0050, RMSE 1.4502, R² 0.9778, 170.5 s/fold**; sweep-wide fold-to-fold SE
was ~$0.002. Inside fold noise → the version decision is closed with evidence and
`requirements.txt` moves to 4.0.1. Outside it → the difference is a finding in its own
right, and the custom-image option reopens with a reason. Cost: one container build and
~17 minutes of laptop time.

**RESULT — 2026-09-02: the two versions are bit-identical. Decision 2 closes on (a).**
The recorded baseline ran on the *host* (Python 3.13.2, macOS), so 4.0.1 in a container
against that record alone would have moved the Spark version and the environment together.
So 4.1.2 ran in the same container as the control, and the version became the only
variable. Two throwaway images off one base, differing by a single `pip install`; the
repository mounted read-only with the results directory overmounted, so no file in the tree
changed. Phase 4b row 2 exactly: 612,608 rows, 5 folds, seed 42, `maxIter=100`,
`maxDepth=5`, smoothing 5, `--cores 8`.

| Run | Spark | Python | `mae_mean` | `rmse_mean` | `r2_mean` | s/fold |
|---|---|---|---|---|---|---|
| recorded baseline (host) | 4.1.2 | 3.13.2 | 0.5201744916052814 | 1.4501613318146291 | 0.977753809523173 | 170.5 |
| parity (container) | **4.0.1** | 3.12.13 | 0.5201744916052814 | 1.4501613318146291 | 0.977753809523173 | 173.9 |
| control (container) | 4.1.2 | 3.12.13 | 0.5201744916052814 | 1.4501613318146291 | 0.977753809523173 | 192.0 |

Every metric agrees to full float64 precision, and so does every standard deviation. The
five per-fold figures agree individually too, not only in the mean — 0.5184, 0.5226,
0.5165, 0.5287, 0.5147. There is no difference to compare against the ~$0.002 fold noise
floor, which is a stronger result than the acceptance condition asked for.

The obvious objections, tested rather than assumed (D-009): the metadata records
`"spark": "4.0.1"` on `Linux-6.10.14-linuxkit-aarch64`, so the run used the pinned version;
the results directory was an empty overmount and the repository was read-only, so nothing
was read back from the old files; and mean *and* standard deviation matching across five
folds requires the fold assignment to have matched, which `--cores 8` fixes by pinning
`defaultParallelism` and therefore the read partitioning.

**Two 4.1.2 behaviours re-measured on 4.0.1, both unchanged.** `TargetEncoder` exists in
4.0.1 (Fact 1 confirmed by measurement, not by the release note). Its `targetType` still
defaults to `"binary"`, so the explicit `"continuous"` stays mandatory. `TargetEncoderModel`
still copies the indexer's nominal `ml_attr` metadata onto its output, and `* 1.0` still
clears it, so the demoting `SQLTransformer` stays mandatory. Neither is a 4.1-only quirk.

**Timing is NOT a version finding — recorded as UNVERIFIED.** The container ran 4.0.1 at
173.9 s/fold and 4.1.2 at 192.0 s/fold, but 4.1.2 ran second, after seventeen minutes of
sustained load on a laptop, and its fold 2 alone took 211.7 s. Run order and thermal state
are not separated by a single ordering, so the 10% gap buys nothing. Only the metrics close
the decision.

**Consequence, pre-registered above and now applied:** `spark/ml/requirements.txt` moves to
`pyspark==4.0.1`. Four statements that read "we run 4.1.2" were corrected with it, in
`src/mllib.py`, `01_mllib_baseline.py`, `tests/unit/ml/test_mllib.py` and the modeling
plan — a stale present-tense claim beside a changed pin is the exact failure D-009 exists
to prevent. Cost: $0, about 40 minutes of laptop time.

### 2.5 Model training — no Vertex AI

**Why not.** The fare model is a persisted estimator in `spark/ml/models/`, served by
nothing online; predictions reach Looker Studio through BigQuery. Vertex AI's value is
managed training infrastructure, tuning services and a registry/serving layer, none of
which the modeling plan calls for. A Serverless batch (MLlib) or a short-lived VM (sklearn,
§8 decision 3) that writes the artefact to `gs://…/ml/models/` is simpler and cheaper. The
cheapest general-purpose Vertex training machine is `e2-standard-4` at ~$0.154–0.161/hour
(VERIFIED on the rebranded page) — not expensive, just unnecessary.

### 2.6 Infrastructure as code — Terraform, reset then import

`terraform/main.tf` is rewritten to declare what the pipeline actually uses: the bucket
(`google_storage_bucket`, name from `var.gcs_bucket_name`, `us-central1`, uniform
bucket-level access, no public access) and the datasets `nyc_taxi_data`,
`nyc_climate_data`, `dbt_prod`, `dbt_dev`, `dbt_ci` (`google_bigquery_dataset`, location per
the 2.1 check). Provider pin `hashicorp/google` 5.6.0 stays unless `terraform init` objects.
State handling is §8 decision 5; the default is local state, **gitignored**, with the three
tracked tfstate files removed from the tree in M0. The CLAUDE.md rule survives unchanged:
never `apply` against resources that exist without `import` first.

### 2.7 Identity — one service account, keyfile locally, ADC on the VM

Per `notes/gcp-setup-runbook.md`: SA `nytaxi-pipeline`, `roles/bigquery.admin` +
`roles/storage.admin`, keyfile at `secrets/gcp-credentials.json` (gitignored), exported
through `GOOGLE_APPLICATION_CREDENTIALS`. CI gets the same key as the `GCP_SA_KEY` secret.
The VM gets the SA *attached* and uses ADC — no keyfile on the VM, in an image, or in git,
ever. The Dataproc batch runs as the same SA (roles to add: UNVERIFIED, see 2.4).
`roles/*.admin` is wider than the pipeline needs; narrowing is optional later work, not a
gate.

---

## 3. Cost model on the free trial

All prices US / `us-central1`; BigQuery in the US multi-region. Fetched from the live pages
on 2026-09-02 unless labelled otherwise.

### 3.1 Unit prices

| Service | Price | Free allowance | Label |
|---|---|---|---|
| BigQuery on-demand queries | $6.25 / TiB | 1 TiB / month | VERIFIED. The "$5/TB" seen in older material was a 25% increase on 2023-07-05 per consistent secondary sources; not a TB→TiB unit artefact (that would be ~10%). Date: CORROBORATED |
| BigQuery active logical storage | $0.000031507 / GiB-hour ≈ $0.023 / GiB-month | 10 GiB / month | VERIFIED. Long-term (90 days unmodified, per partition) ≈ $0.016 |
| BigQuery Storage Read API (what the Spark connector uses) | $1.10 / TiB | 300 TiB / month | VERIFIED |
| Dataproc Serverless, DCU-hour | $0.06 standard, $0.089 premium; per second, 1-minute minimum | none | VERIFIED. All-inclusive — no separate VM charge |
| Dataproc Serverless shuffle storage | $0.000054795 / GiB-hour standard | none | VERIFIED |
| Serverless minimum footprint: 12 DCU (driver 4 vCPU/16 GB + 2 executors 4 vCPU/16 GB; 1 vCPU = 0.6 DCU, RAM 0.1 DCU/GB) | ⇒ **$0.72 / hour at the floor** | — | CORROBORATED (composition from secondary summaries of the pricing page) |
| Dataproc classic management fee | $0.01 / vCPU-hour + Compute Engine | none | VERIFIED — rejected anyway |
| Cloud Composer | $0.06 / 1,000 milliDCU-hour; no scale-to-zero | none | VERIFIED rate; monthly floor UNVERIFIED (~$300–450 secondary) — rejected |
| GCS standard storage, US regional | not captured | 5 GB-month, 5,000 class A + 50,000 class B ops, 100 GB egress from NA | Allowance VERIFIED; unit price UNVERIFIED — at single-digit GB it is cents |
| Compute Engine, the Airflow VM | not captured for GCE proper | 1 `e2-micro`/month + 30 GB-month standard PD + 1 GB egress | Allowance VERIFIED. Proxy: Vertex's `e2-standard-4` at $0.154–0.161/h (VERIFIED there, not on the GCE page). **Verify on the Compute Engine page before M5** |

### 3.2 What the pipeline will consume

| Item | Estimate | Basis |
|---|---|---|
| Raw parquet in GCS, 2015-01–2016-12 | ~4–5 GB | `yellow_tripdata_2015-06.parquet` is 172 MB (measured on download); 24 yellow months ≈ 4.1 GB, green is far smaller. Sits on the edge of the 5 GB free allowance — expect cents, not dollars |
| ML samples in GCS | ~1–2 GB | `sample_full` + `sample_work_train`; sizes not measured, ASSUMED from row counts |
| `dbt_prod.fact_trips` logical storage | **unknown; ASSUMED 15–30 GiB** | BigQuery logical bytes are *uncompressed*; the 7.1 GB parquet is compressed. At 30 GiB: (30 − 10) × $0.023 ≈ **$0.46 / month**. Measure after M3 |
| One `dbt build --target prod` | well under 1 TiB | scans ~5 GB of raw parquet plus the marts; **free** |
| Dashboard / ad-hoc queries | free until ~40 full scans of a 25 GiB table per month | partition + cluster (2.2) shrink most scans; **free** |
| Spark reads of `fact_trips` via the connector | free | 300 TiB/month allowance against a table of tens of GiB |
| Serverless: `01_mllib_baseline.py` at work-612k (M4 smoke) | 989 s locally ⇒ ~17 min at 12 DCU ⇒ **~$0.20** | measured wall time × $0.72/h; assumes Serverless is no slower than the 11-core laptop — ASSUMED |
| Serverless: MLlib GBT on `sample_full` (10.2M train), 5 folds | 170.5 s/fold × 16.6 ≈ 47 min/fold ⇒ ~4 h ⇒ **~$3** | linear extrapolation of §5c's scale factor; a 10× miss is still $30 |
| Serverless: `00_prep_spark.py` on all 128.78M rows from BigQuery | not measured; ASSUMED < 1 h ⇒ < $1 | |
| sklearn §5c scope (top 4 + corridor-dropped champion) | ~8 h single-machine ceiling; on a short-lived VM ⇒ **single-digit dollars** at any plausible 8-vCPU on-demand rate | §5c measured 8.2 h for all 14 models; the scoped run is dominated by `stacking` (~4 h). VM price UNVERIFIED |

**Total for the whole migration, if every rule above is followed: low tens of dollars.**
The $300 credit is not at risk from work. It is at risk from *idleness*.

### 3.3 The credit-exhaustion story

| How the credit dies | Rate | Time to $300 |
|---|---|---|
| One 4-vCPU VM left running | ~$0.16/h proxy × 730 h ≈ $115/month | **under 3 months** — i.e. the whole 90-day trial |
| A Composer environment left up | ≥ $300/month (UNVERIFIED floor) | **≤ 1 month** |
| A classic Dataproc cluster left up (2 × n-standard-4) | VM cost + $0.08/h fee | weeks |
| Everything in this plan, run as written | tens of dollars | never, inside 90 days |

Controls, all set in M1 before anything else is created: a Cloud Billing budget on the
project with alerts at $50 / $150 / $250; the VM's stop rule (2.3); Serverless only for
Spark; a `gcloud compute instances list` check in the post-run steps of M4 and M5. The
90-day clock starts at signup — do not sign up before M0 is finished (§8 decision 6).

---

## 4. Migration sequence

Smallest reversible steps first; each phase has a gate and a rollback; **the per-step
review gate in CLAUDE.md applies inside every phase** — a phase is a grouping for this
document, not a batch to run unattended. TDD rule applies: where a phase changes code or
config that a test can pin, the failing test lands first.

### M0 — Hygiene, local only, no cloud, fully reversible

- [x] **Test first:** extend `tests/unit/dags/test_dag_config.py` (or a new
      `tests/unit/test_stale_ids.py`) with two guards — (a) no file under `terraform/`
      matching `*.tfstate*` is tracked by git; (b) `dbt/ny_taxi_analytics/models/staging/*.yml`
      contains no literal `dtc-de-` project ID in a `database:` line (read-only scan of the
      submodule; that is allowed). Both fail today. **Done `6315b87`:** new
      `tests/unit/test_stale_ids.py`. Guard (a) passes now. Guard (b) is
      `xfail(strict=True)`, blocked on the upstream push, and carries positive controls so
      a pattern that stopped matching cannot pass silently.
- [x] Remove the three tfstate files from the index (`git rm --cached`), add
      `terraform/*.tfstate*` and `terraform/.terraform/` to `.gitignore`. Keep the files on
      disk until M1 confirms the fresh state, then delete. **Done `6315b87`:**
      `git ls-files -- 'terraform/*.tfstate*'` is empty, both ignore patterns are in
      `.gitignore`, and all three files are still on disk as intended. **M1 deletes them.**
- [x] Rewrite `terraform/main.tf` per 2.6; keep `variables.tf` defaults. `terraform
      validate` only — no `init` against a backend yet. **Done `0b6421c`:** the bucket plus
      the five datasets, `demo_dataset` gone, `variables.tf` untouched (`var.bq_dataset_name`
      is now unreferenced, left deliberately). `terraform init -backend=false` +
      `terraform validate` → Success; no plan, no apply. The provider lock file is committed
      with darwin_arm64 *and* linux_amd64 hashes so `init` also works on the M1 VM.
- [x] **Upstream** in `ny_taxi_analytics`: **delete** the `database:` line from both
      staging schema files — *corrected 2026-09-02, see the note below;* the `fact_trips`
      partition/cluster config (2.2) after reading its current block. Merge there, then bump
      the submodule pointer here.

      **DONE 2026-09-02 — upstream `305868f`, pointer moved off `d11219d`.** The owner
      pushed all three changes in one commit. Both guards in `tests/unit/test_stale_ids.py`
      reported `XPASS(strict)` on the bump, exactly as designed, and the
      `xfail(strict=True)` marker was deleted in the same commit that recorded the pointer.
      They now assert plainly and stay as regression guards. The record of what was applied,
      kept for the next reader:

      The `database:`
      half was prepared and dry-run: it is two deletions, verified to apply cleanly against
      the currently pinned commit `d11219d`, and re-verified 2026-09-02 against the
      installed dbt 1.11.11 (`dbt/parser/sources.py:154,158` —
      `database=(source.database or default_database)`), so an absent `database` inherits
      the profile's project. In the `ny_taxi_analytics` clone:

      ```diff
      --- a/models/staging/schema_taxi.yml
      +++ b/models/staging/schema_taxi.yml
      @@ sources: - name: staging
      -    database: dtc-de-project-492321
           schema: nyc_taxi_data

      --- a/models/staging/schema_climate.yml
      +++ b/models/staging/schema_climate.yml
      @@ sources: - name: staging
      -    database: dtc-de-project-492321 # new dataset name
           schema: nyc_climate_data
      ```

      Keep `schema:` — that is the dataset, and it differs from the target. The stray
      `# new dataset name` comment goes with the line it annotated; it labelled a project
      as a dataset, which is the vocabulary collision that caused this. Then here:
      `git submodule update --remote dbt/ny_taxi_analytics && git add dbt/ny_taxi_analytics`.
      The moment the pointer bumps, the two `xfail(strict=True)` guards in
      `tests/unit/test_stale_ids.py` fail hard — that failure is the instruction to delete
      the marker.

      **The `fact_trips` half, also prepared.** 2.2 said to read the config block before
      writing it. Read: line 1 is `{{ config(materialized='table') }}` and nothing else, so
      this adds and overwrites nothing. Dry-run against `d11219d`; it applies cleanly.

      ```diff
      --- a/models/core/fact_trips.sql
      +++ b/models/core/fact_trips.sql
      -{{ config(materialized='table') }}
      +{{ config(
      +    materialized='table',
      +    partition_by={"field": "pickup_datetime", "data_type": "timestamp",
      +                  "granularity": "month"},
      +    cluster_by=["pickup_locationid", "dropoff_locationid"]
      +) }}
      ```

      All three columns exist in the model's own `select`, and `pickup_datetime` is a real
      timestamp — staging builds it with `cast(lpep_pickup_datetime as timestamp)` — so the
      `data_type` is verified, not assumed. Both halves go in **one** submodule PR, then one
      pointer bump here.
- [x] Update `.github/workflows/dbt.yml` to export `GCP_PROJECT_ID` if it does not already.
      **Done `c091af8`:** it did not, so every CI run silently targeted the dead fallback
      project. The auth step now resolves it — `vars.GCP_PROJECT_ID` first, else the
      `project_id` inside `GCP_SA_KEY` itself, else a hard error — and exports it through
      `$GITHUB_ENV`. Empty is never exported: dbt's `env_var()` returns `""` rather than its
      default when the name is set. Unverified end to end; that needs M1's live project.
- [x] The Spark 4.0.1 parity test from 2.4 (throwaway container build; no file in the tree
      changes unless the result says so). **Done 2026-09-02: bit-identical.** Full numbers,
      method and objections in 2.4. The result said so, so the pre-registered consequence
      applied: `spark/ml/requirements.txt` → `pyspark==4.0.1`, plus the four "we run 4.1.2"
      statements it makes stale. Open decision 2 is taken; **D-011** records it.
- **Gate:** `pytest tests/` — unit count up by the new guards, all green; the 9 integration
  failures unchanged. **Rollback:** `git checkout` — nothing outside the repo moved.
- **Cost:** $0.

> **Correction — 2026-09-02, the `database:` fix.** An earlier draft of this bullet said to
> write `database: "{{ env_var('GCP_PROJECT_ID') }}"` into both staging schema files. That
> works, but it is the wrong fix. **Delete the line instead.**
>
> The defect is a vocabulary collision, not a typo. dbt names things generically and
> BigQuery names them concretely, so the same two concepts carry two sets of words:
> `database:` in a schema YAML is the **project**, and `schema:` is the **dataset**, while
> `dbt/profiles.yml` calls the identical things `project` and `dataset`. The collision has
> already produced a wrong comment in the repository — `schema_climate.yml` reads
> `database: <project> # new dataset name`, labelling a project as a dataset.
>
> Verified against the installed dbt 1.11.11, `dbt/parser/sources.py:158`:
>
> ```python
> default_database = self.root_project.credentials.database
> ...
> database=(source.database or default_database),
> ```
>
> An absent `database` inherits the profile's `project`, which already resolves from
> `GCP_PROJECT_ID`. Templatising the source YAML would duplicate a value the profile
> already owns — a second place to edit and a second place to drift, which is the config
> equivalent of the raw/derived duplicate that `d83141b` cost us. **`schema:` stays**: the
> sources read `nyc_taxi_data` and `nyc_climate_data`, which are not the target dataset.
>
> Still blocked upstream either way. Deleting a line inside `dbt/ny_taxi_analytics` is
> still editing the submodule, and pushing there is the owner's.

### M1 — Provision, via Terraform, one bucket and five datasets

**Opening measurement, 2026-09-02, keyfile `secrets/gcp-credentials.json`.** The statement in
`CLAUDE.md` that "No GCP project is provisioned" is **false as measured**. The project is
live, billing is enabled, and it already holds the migrated data.

| Thing | Measured |
| --- | --- |
| Keyfile project | `dtc-de-project-506916`, SA `dtc-de-course@dtc-de-project-506916.iam.gserviceaccount.com` |
| `gcloud` active account | `saggysimmba@gmail.com`; `core/project` = `dtc-de-project-506916` |
| Billing account | `01E445-18A569-B9097E` "My Billing Account", OPEN, linked (`billingEnabled: true`) |
| BigQuery datasets | **`dbt_prod` only**, location `US`. `nyc_taxi_data`, `nyc_climate_data`, `dbt_dev`, `dbt_ci` are absent |
| Tables in `dbt_prod` | `fact_trips` 128,781,646 rows / 58.75 GB; `dim_monthly_zones_revenue` 11,572; `dim_zones` 265; `taxi_zone_lookup` 265 |
| GCS bucket | `primary-data-dtc-506916`, location **`US` multi-region**, STANDARD |
| Bucket contents | 204 objects, 7.05 GiB, all under `dbt_prod_restore/fact_trips/` |
| `billingbudgets.googleapis.com` | **not enabled** — `gcloud billing budgets list` fails `SERVICE_DISABLED` |

Three consequences the plan did not anticipate.

1. **M1 is not a $0 step on this project.** 58.75 GB in BigQuery and 7.05 GiB in GCS already
   accrue. Rough monthly list price: BigQuery active storage at $0.02/GB-month over the 10 GB
   free allowance ≈ **$0.98/month**; GCS standard US multi-region ≈ **$0.19/month**. Call it
   **~$1.20/month, already running**. The unit prices are UNVERIFIED (no primary page
   captured); the storage figures are measured.
2. **The live bucket's location contradicts `terraform/main.tf`.** The bucket is `US`
   multi-region; `main.tf` sets the bucket `location = var.region` = `us-central1`. A bucket's
   location is immutable, so importing this bucket and planning would show a **replace** —
   which destroys 7.05 GiB. On branch (A) the bucket's location must be set to `US` before any
   plan. This is separate from the dataset-location question in 2.1, which is resolved.
3. **One of the five datasets exists, not none.** A branch-(A) run imports `dbt_prod` and
   creates the other four.

**Branch chosen: (A) reuse `dtc-de-project-506916`** (owner, 2026-09-02). The live bucket and
`dbt_prod` are imported; the four missing datasets are created.

**Budget guard — DONE, 2026-09-02, before any resource was created.**

```
gcloud services enable billingbudgets.googleapis.com --project=dtc-de-project-506916
gcloud billing budgets create --billing-account=01E445-18A569-B9097E \
  --display-name="nyc-taxi-guard-50usd" --budget-amount=50USD \
  --threshold-rule=percent=1.0 --threshold-rule=percent=1.0,basis=forecasted-spend
```

The same command ran for 150 and 250. Verified by `gcloud billing budgets list`:

| Budget | Amount | Thresholds |
| --- | --- | --- |
| `nyc-taxi-guard-50usd` | 50 USD | 100% actual, 100% forecast |
| `nyc-taxi-guard-150usd` | 150 USD | 100% actual, 100% forecast |
| `nyc-taxi-guard-250usd` | 250 USD | 100% actual, 100% forecast |

Notes on the guard, all measured.

- The budgets are **billing-account wide**, not project-scoped. `gcloud billing budgets`
  takes `--billing-account`; the earlier UNVERIFIED note is now settled — it does target the
  billing account, and `billingbudgets.googleapis.com` was indeed disabled and had to be
  enabled first.
- Notification goes to the default IAM recipients (billing admins) by email.
  `disableDefaultIamRecipients` is unset.
- **A second project shares this billing account:** `project-672ad9c7-bfa8-470e-9e1`, billing
  enabled. Its spend counts against these budgets. The owner should check what it is.
- **The project is `ACTIVE`.** The stored note that the account was disabled with an appeal
  pending is stale.


- [ ] Follow `notes/gcp-setup-runbook.md` up to and including the keyfile at
      `secrets/gcp-credentials.json`; add the **budget alerts** (3.3) as the first
      action after billing is linked. Enable `bigquery`, `bigquerystorage`, `storage`,
      plus `dataproc.googleapis.com` and `compute.googleapis.com` for M4/M5.
- [ ] Resolve the dataset-location question (2.1) before creating anything.
- [ ] `terraform init` (fresh state, new lineage), `terraform plan` — must show creates
      only. If the project already holds a resource with one of these names, `terraform
      import` it first (the standing rule). `terraform apply`.
- **Gate:** `tests/integration/test_gcs.py::…bucket reachable` passes; dataset-exists tests
  in `test_bigquery.py` pass; file-count and row-count tests still fail (nothing
  ingested). Expected: 9 → 5 or 6 failures.
- **Rollback:** `terraform destroy` — empty bucket and empty datasets, seconds to undo.
- **Cost:** $0 (empty resources).

### M2 — Prove the ingest and external-table DAGs from the laptop

- [ ] Confirm how `airflow/docker-compose.yaml` passes `GOOGLE_APPLICATION_CREDENTIALS`
      and the keyfile mount into the containers (unread in the research). Set
      `GCP_PROJECT_ID`, `GCP_GCS_BUCKET` in the compose env to the M1 values.
- [ ] `docker compose -f airflow/docker-compose.yaml up --build` locally. Trigger, in
      order and one at a time: `nyc_taxi_zone_ingestion_dag`, `nyc_climate_data_ingestion_dag`,
      `nyc_green_taxi_data_ingestion_dag`, `nyc_taxi_data_ingestion_dag` (yellow, the big
      one — 24 × ~170 MB through the laptop's uplink). Each triggers its external-table DAG.
- [ ] Record per-file byte sizes and row counts from the GCS listing into
      `notes/gcp-reference.md` — this is the archive fingerprint §6.1 depends on.
- **Gate:** `test_gcs.py` 24 + 24 parquet files, zone CSV, climate parquet;
  `test_bigquery.py::TestExternalTablesQueryable` rows > 0. Failures: → 1 (`test_dbt`).
- **Rollback:** delete the prefixes; external tables are `CREATE OR REPLACE`.
- **Cost:** ~5 GB in GCS, on the free allowance or cents.

### M3 — dbt marts in the cloud, and the reproducibility check that matters

- [ ] `dbt_build_marts` from the local compose stack (target `prod`), or the CLI form in
      CLAUDE.md. First `dbt build --target dev` to `dbt_dev` if a cheap dry run is wanted.
- [ ] **The check:** `SELECT COUNT(*) FROM dbt_prod.fact_trips` must return
      **128,781,646** — the local backup's exact count, verified against the dashboard on
      2026-07-10. Equal → the cloud rebuild reproduces the local modeling input and the
      backup is a *copy*, not a *fork*. Unequal → §6.1's archive dependency has bitten,
      and the difference is the first thing to report.
- [ ] Measure `fact_trips` logical bytes and record it against the 3.2 assumption.
- [ ] Reconnect Looker Studio to `dbt_prod`.
- [ ] Push the branch so CI's `dbt build --target ci` runs once with `GCP_SA_KEY` set
      (the owner pushes — audit item 2).
- **Gate:** `pytest tests/` — 0 integration failures. First time in the project's history.
- **Rollback:** drop `dbt_prod` tables; rebuild is one DAG run.
- **Cost:** under $1/month storage; queries free.

### M4 — Spark on Dataproc Serverless: smoke, then §5c

- [ ] Look up and grant the Serverless batch's service-account roles (UNVERIFIED in 2.4).
- [ ] Upload `sample_work_train.parquet` (612,608 rows) to `gs://…/ml/samples/`.
- [ ] Add a `--input` / `--output` URI pair to `01_mllib_baseline.py` if it does not take
      one (it reads local paths today); test first, in `tests/unit/ml/test_mllib.py`.
- [ ] Submit `01_mllib_baseline.py` as a batch on runtime 3.0 with
      `spark.jars.packages=com.google.cloud.spark:spark-4.0-bigquery:0.45.0` (the connector
      is not exercised by this run; the point is that the coordinate resolves). Compare to
      the 2.4 baseline exactly as in the local 4.0.1 parity test.
- [ ] `00_prep_spark.py` reading `dbt_prod.fact_trips` through the connector instead of
      the local backup; assert the 128,781,646 count and the p99 caps in `prep_stats.json`
      (Yellow $52 / 18.7 mi, Green $45 / 14.15 mi) match the local run.
- [ ] §5c MLlib arm on `sample_full` — after §5's encoder work decides *which* MLlib arm.
- [ ] `gcloud dataproc batches list` — nothing running; `gcloud compute instances list` —
      nothing exists.
- **Gate:** smoke run inside fold noise of the local result; prep stats identical.
- **Rollback:** delete the `ml/` prefix. Serverless leaves nothing behind.
- **Cost:** ~$0.20 smoke; ~$3 MLlib at scale; < $1 prep (3.2).

### M5 — The Airflow VM, and D-006 closed

- [ ] Verify the GCE price for the chosen machine on the Compute Engine page (3.1).
      Minimum size: the compose stack runs a Postgres, scheduler, webserver, worker and
      triggerer; **the 1 GB `e2-micro` is ASSUMED too small** — an `e2-standard-2` (8 GB)
      is the first thing to try. ≤ 30 GB boot disk so a stopped VM is free.
- [ ] Create the VM with SA `nytaxi-pipeline` attached, `cloud-platform` scope. Clone the
      repo on the VM, `docker compose up --build` **there** (amd64 — never push a Mac
      build). No keyfile is copied; `GOOGLE_APPLICATION_CREDENTIALS` is unset and the
      client libraries use ADC. Confirm `dbt/profiles.yml`'s `method: service-account` +
      `keyfile` lines handle an unset variable — UNVERIFIED; the `oauth` method is the
      fallback for the VM target.
- [ ] Trigger one ingest DAG end to end from the VM's Airflow UI; it must reach the
      external-table DAG. Then **stop the VM.**
- **Gate:** the E2E tier in CLAUDE.md's table; VM state `TERMINATED` in
  `gcloud compute instances list`.
- **Rollback:** delete the VM. Nothing else depends on it.
- **Cost:** hours × verified rate while running; $0 stopped.

### M6 — Documents

- [ ] CLAUDE.md: replace the "GCP — not provisioned" section; keep "Deployment split"
      (D-006 resolved: keep). `notes/gcp-reference.md`: live layout, fingerprint table,
      the post-run cost checks. `notes/decisions.md`: D-006 entry closed with this file as
      the reason; §5's D2 reversal recorded as a new entry.
- [ ] `notes/2026-08-22-repo-audit.md`: check off 5, 6, 8; the workstream order (item 6)
      is: **this migration through M3 → modeling §5 encoder + §5c → dashboard v3.**
      Item 8's "one next pointer" is: CLAUDE.md → this file → the modeling plan Status.
- [x] `spark/ml/requirements.txt` to `pyspark==4.0.1` if M0's parity test passed.
      **Done early, in M0 (`cc8b7a9`).** 2.4 pre-registered the pin move as the consequence
      of a result inside fold noise, and the result was bit-identical, so it landed with the
      measurement rather than waiting for M6. The four "we run 4.1.2" statements it made
      stale moved with it.
- **Gate:** `pytest tests/` green; `test_docker_runtime.py` pins still hold.

---

## 5. The PySpark modeling question

### 5.1 What is being reversed

Modeling plan §10 (2026-07-30) adopted **D2: `preprocess.py`, `evaluate.py` and the
Phase-4 sweep stay scikit-learn**, on three grounds: at 765K rows Spark is slower on one
machine; Spark ML lacked a `TargetEncoder`; a third of the model families do not exist in
MLlib. The second ground was wrong (`TargetEncoder` exists since 4.0.0; corrected
2026-08-09). The owner now asks for modeling in PySpark. This plan treats that as a
requirement and records here whether the reversal is justified on evidence. **It is not
yet — and the thing that would justify it is buildable.**

### 5.2 The measured evidence (identical 612,608 rows, 5-fold CV, 2026-09-01)

| Row | Stack | `od_corridor` | MAE | RMSE | R² | s/fold |
|---|---|---|---|---|---|---|
| 1 | sklearn `lightgbm` | target-encoded, **cross-fitted** | **0.3503** ±0.0015 | **1.0516** | **0.9883** | **1.0** |
| 3 | MLlib GBT | dropped | 0.4828 ±0.0038 | 1.2582 | 0.9833 | 51.4 |
| 2 | MLlib GBT | target-encoded, smoothing 5, **not cross-fitted** | 0.5202 ±0.0050 | 1.4502 | 0.9778 | 170.5 |

Seven-arm smoothing sweep (200,000 rows, 3 folds, `maxIter=20`, mean MAE): corridor
dropped **0.6329**; s=5 0.6650; s=1 0.6713; s=0.067 0.6908; s=100 0.7094; s=20 0.7102;
s=500 0.7236. **No smoothing value beats dropping the corridor.** The result replicates.

**Root cause, identified but not yet isolated:** sklearn's `TargetEncoder` cross-fits
inside `fit_transform` (measured 2026-08-09 on a 12-row worst case: encoded values exclude
the row's own target; a spy transformer confirmed `Pipeline.fit` takes that path). Spark's
`TargetEncoder.fit` takes the plain per-category mean, so a training row's own fare enters
its own feature. With 5,373 of 18,668 corridors holding exactly one trip (28.8% of
corridors), the encoding for those rows *is* the label, the booster over-trusts it, and
the feature goes net-negative. §5b says plainly that the run establishes the effect, not
the mechanism, and that isolating the mechanism needs "a cross-fitted encoding computed
outside MLlib and fed in as a plain column".

That is the thing which has to exist before PySpark modeling is defensible. Without it, a
PySpark model is one that cannot use the project's strongest feature.

### 5.3 The out-of-fold target encoder in PySpark — specification

Module `spark/ml/src/oof_encode.py`, one pure function over DataFrames, unit-tested first
with a local `SparkSession` fixture (these tests will take seconds, not milliseconds; that
is a known cost — §10 said so — and it is confined to one test module).

```
oof_target_encode(df, key_col="od_corridor", target_col="fare_capped",
                  fold_col="oof_fold", k=5, smoothing=5.0) -> DataFrame
```

1. **Fold assignment from a stable key, not `monotonically_increasing_id()`.** That
   function is not stable across re-materialisation, so a bug would be silent. Use
   `F.pmod(F.crc32(F.col(row_key)), k)`. **Finding:** `00_prep_spark.py` drops `tripid`
   (it is in the ID/unused drop list), so `sample_work_train.parquet` carries **no stable
   row key**. The prep's `keep` list needs `tripid` (or a persisted row id) — the same
   prep re-run §4a's deferred temporal split is already waiting on (`pickup_datetime`).
   Do both in one re-run; note the re-run regenerates the samples and therefore the
   holdout partition, so it happens *before* Phase 5, never after.
2. For each fold `f`: `groupBy(key).agg(sum(target), count())` over rows with
   `fold != f`; join onto rows with `fold == f`; encode as
   `(n·mean_cat + smoothing·global_mean) / (n + smoothing)` — the formula
   `self_leakage_weight()` in `mllib.py` already documents — with `global_mean` also taken
   from the complement. Keys absent from the complement → `global_mean`.
3. Union the k frames. Every training row now carries an encoding computed without its
   own label. k group-by + join passes; at 612K rows this is seconds, at 10.2M it is
   minutes.
4. Test-fold / holdout / prediction time: encode with the **full training set's** means,
   no fold exclusion — exactly what sklearn's `transform()` does. Unknown → global mean,
   matching both stacks today (§5b: "unknown categories agree").
5. Feed the column to the existing `mllib.py` pipeline as a plain double in place of the
   `StringIndexer` + `TargetEncoder` stages. Remove the stages, keep everything else — the
   parity rule holds.

**Tests to write first** (`tests/unit/ml/test_oof_encode.py`): the §5b 12-row worst case
(every corridor unique, targets 10–21, mean 15.5) — no encoded value may equal its row's
own target, and each must equal the smoothed complement mean; a repeated corridor across
folds encodes from the other folds only; an unseen key at transform time returns the
global mean; the same input yields the same output across two `SparkSession`s (stability
of the fold key); `smoothing=0` with a key present in every complement returns the plain
complement mean.

**Nested CV cost, stated so it is not discovered later.** The outer harness is 5 folds;
each outer training half needs its own OOF encoding fitted on those rows only, so a
5-fold outer run costs 5 × k inner passes. At k=5 that is 25 group-by/join passes per
model. Fine on Serverless at $0.72/h; slow in the laptop TDD loop. The §10 warning stands:
this is the one place a bug is silent leakage that inflates R². The tests above are the
defence; the second defence is the acceptance criterion below, which cannot be gamed
upward by leakage because the *test* folds are untouched.

### 5.4 Acceptance criterion — what would justify the reversal

Run row 2 again with the OOF column: `mllib_gbt_oof@work612k`, same rows, same folds as
`01_mllib_baseline.py`, same GBT settings.

| Outcome | Reading | Consequence |
|---|---|---|
| MAE materially below row 3 (0.4828) — the corridor turns net-positive | mechanism isolated: it was the uncross-fitted encoder | the reversal of D2 has an evidential basis; §5c runs the MLlib arm with the OOF column; the PySpark stack is a fair comparator |
| MAE between row 2 and row 3 | partial: encoder was part of the story, MLlib's GBT the rest | report it; MLlib arm runs with the OOF column, but "PySpark modeling" stays a demonstrated *baseline*, not the champion track |
| MAE at or above row 2 | the mechanism was not the encoder | negative result, reported; D2 stands and is re-recorded with the new reason |

Either way the mechanism question §5b left open gets closed, which is a result the
write-up needs regardless of the stack decision.

### 5.5 When PySpark is the right tool for the *model*, and what the owner gives up

**Memory is the boundary, not row count as such.**

| Frame | Rows (train) | Feature frame | Fits where |
|---|---|---|---|
| `sample_work` | 612,608 | ~0.24 GB (scaled from the 500K measurement) | anywhere |
| `sample_full` | 10.2M | **3.9 GB** measured extrapolation; ~1.3 GB after the §5c category-dtype enabler (four string columns are ~80% of the footprint) | the 18 GiB laptop, comfortably, after the enabler |
| all of `fact_trips` | ~103M | ~39 GB; ~13 GB with category dtype (ASSUMED — linear scaling of the same measurement) | a single 64 GB VM; not the laptop |
| the full TLC yellow archive 2009–2026 | roughly an order of magnitude more (ASSUMED; not counted in the research) | past any single machine the trial buys |

sklearn `lightgbm` is ~1.4 min per fold at 10.2M rows by §5c's extrapolation and would be
~15 min at 103M. So: **sklearn holds through the full 12.75M sample on the laptop, and
through all 128.78M rows on one 64 GB VM. PySpark modeling becomes necessary — not merely
possible — beyond about 10⁸ rows at the current 15-column width, or when the training
data becomes the multi-year archive (§6).** The one unknown §5c names still stands:
sklearn's cross-fitted `TargetEncoder` over 19,953 levels at 10.2M rows has never been
measured — probe it with `--only lightgbm --sample full` before sizing anything.

**What a full move to PySpark gives up at today's sizes** (measured unless marked):

- 1.5× accuracy and 50–167× wall time on identical rows, until §5.4 shows the OOF column
  recovers the accuracy half. The wall-time half does not recover on one machine — JVM,
  serialisation and shuffle are overhead with no cluster to amortise (§10).
- `ExtraTrees`, `BaggingRegressor`, `StackingRegressor` — three of the top-four-by-RMSE
  scope for §5c has `stacking` and `extra_trees` in it. XGBoost via `xgboost.spark` and
  LightGBM via SynapseML exist but are heavier dependencies (ASSUMED untested here).
- `cross_validate`'s multi-metric, per-fold arrays. `CrossValidator.evaluator` takes one
  `Evaluator` (VERIFIED, Spark JavaDoc); `avgMetrics` is per param-map for that one
  metric; `subModels` only with `collectSubModels=True`; `stdMetrics` since 3.3.0
  (CORROBORATED). Three metrics means three runs or the hand-rolled loop `mllib.py`
  already has — so `evaluate.py`'s contract is kept by *keeping* the hand-rolled loop,
  not by adopting `CrossValidator`, which is a hyperparameter-search tool.
- Millisecond unit tests. Every `SparkSession` fixture costs seconds; the TDD loop that
  carried Phases 1–3 gets slower in proportion to how much moves.

**Recommendation, stated the way the question was asked.** PySpark for the pipeline —
prep, feature engineering (D1, already adopted), the OOF encoder, the MLlib arm of every
comparison, and the §5c MLlib run on Serverless. sklearn for the champion model until the
training frame exceeds a single machine's memory, which at this width is the full
128.78M-row table on a 64 GB VM. Build the OOF encoder now regardless: it is the
prerequisite for the MLlib arm to be a fair comparison, it closes the open mechanism
question, and it is the deliverable that makes "modeling in PySpark" a defensible portfolio
claim rather than a weaker model with a Spark logo on it. Record this as a new entry in
`notes/decisions.md` that supersedes D2 in the modeling plan, with §5.4's table as the
condition.

---

## 6. Data growth

### 6.1 The TLC schema question — resolved, with a dependency

The widely repeated claim that TLC switched from pickup/dropoff latitude-longitude to
`PULocationID`/`DOLocationID` in the July 2016 files, leaving this project's 2015-01–2016-12
window straddling a schema change, **does not apply to the archive that is downloadable
today.** Checked 2026-09-02 by downloading from TLC's own CDN
(`d37ci6vzurychx.cloudfront.net/trip-data/`):

- `yellow_tripdata_2015-06`, `2016-06`, `2016-07`, `2016-12` and `green_tripdata_2015-06`
  all carry `PULocationID`/`DOLocationID` and **no** lat/long columns.
- The full `yellow_tripdata_2015-06.parquet` (172 MB, 12.3M rows) was inspected with
  pyarrow: zero nulls in either ID column, values 1–265, realistic frequencies (zone 79:
  393,995 rows). Real backfilled zone IDs, not placeholders.
- The current data dictionaries (dated 2025-03-18) list only the ID columns. Wayback
  snapshots show the lat/long-era dictionary still being served in July 2017, so the PDFs
  lagged the data by a year and are not a source for the cutover date.
- The CSV→Parquet migration happened between 2022-03-03 and 2022-05-31 (Wayback bracket);
  the retroactive zone-ID backfill evidently rode along with it. The exact date is
  UNVERIFIED and does not matter here.

**Status: risk closed for the 2015–2016 window.** The dbt staging models and the
external-table wildcards see one schema.

**The dependency it creates.** The project ingests TLC's *reprocessed* archive. TLC has
rewritten history once; nothing guarantees a later download is byte-identical to today's
(sizes, row counts, or a further backfill). Hence the fingerprint in M2 (per-file bytes
and row counts) and the M3 check (`COUNT(*) = 128,781,646`). If a re-ingest ever disagrees
with the fingerprint, the modeling samples are regenerated from the new archive and the
change is reported, not absorbed.

### 6.2 Later years

Availability (VERIFIED, TLC page 2026-09-02): yellow 2009-01 → 2026-05; green 2013-08 →
2026-05; FHV 2015-01 → 2026-04 (base-license, pickup time, zone — no fare); FHVHV
(Uber/Lyft/Via/Juno) 2019-02 → 2026-05, richer schema, "separate (and more detailed)
dataset".

What extending `INGEST_END_DATE` past 2016-12 costs and risks:

- Storage and query stay free or near-free at any span the trial would see: ~170 MB per
  yellow month, ~2 GB a year; BigQuery logical growth of perhaps 15 GiB a year (ASSUMED
  from the 3.2 estimate) at $0.023/GiB — dollars a month at a decade.
- **Column drift in later years** — the external tables are defined by wildcard over all
  files in a prefix, and additional or retyped columns in newer files can break the
  external table or the staging view. Which columns and when (congestion surcharge, airport
  fee, `passenger_count` nullability) is **ASSUMED from general knowledge, not checked in
  the research**; inspect the Parquet schema of the first file of each new year before
  ingesting it, the same way 6.1 was settled.
- **Rate-card changes.** A metered fare is near-deterministic in distance and time under
  one rate card; the 2015–16 window sits under one (§4a). Later years do not (dates
  UNVERIFIED here). A model trained across a rate change without a `year`/rate feature
  or a temporal split is wrong by construction — so extending the window is the moment
  §4a's deferred temporal test set stops being deferrable.
- This is also the point at which §5.5's memory boundary moves: three or four years of
  yellow alone is the ~10⁸-row regime where the MLlib arm stops being a baseline and
  starts being the only stack that runs.

### 6.3 OSM / OSRM routing integration — scoped, not scheduled

README Future Scope already names the design: a 263×263 zone-centroid route
distance/duration matrix from one OSRM `/table` call, joined into the feature contract as a
corridor-level feature. Facts gathered (all CORROBORATED from OSRM issues/community
sources, none from an official OSRM doc fetch):

- Image `ghcr.io/project-osrm/osrm-backend`; pipeline `osrm-extract` (car profile) →
  `osrm-partition` → `osrm-customize` → `osrm-routed --algorithm mld`.
- `--max-table-size` defaults to 100 coordinates; **263 needs it raised explicitly.** A GET
  URL-length ceiling around 350–400 pairs is well above 263.
- Geofabrik `new-york-latest.osm.pbf` is **471 MB and covers New York State**, not the
  city. Clip with `osmium extract` and a five-borough polygon first, or accept the
  superset. Rule-of-thumb RAM ≈ 5× the `.pbf` ⇒ ~2.5 GB; disk single-digit GB.

**Where it runs: locally, in Docker, once.** The output is one small table (69,169 rows),
which goes to GCS and becomes a dbt seed or a `dim_zone_routes` model upstream, joined in
`fact_trips` on the two zone IDs. Nothing about it belongs on the Airflow VM or on
Serverless. Centroids come from the zone geometry noted in the project's reference-data
memory (NYC Taxi Zones GeoParquet), not from the CSV lookup, which has no coordinates.

**Modeling consequence.** A route feature is a new version of the 15-feature contract:
new sweep on CV, new leaderboard, the sealed holdout still untouched. It is a post-Phase-5
item by the modeling plan's own order and this plan does not move it.

---

## 7. Risks, and what would make each choice wrong

| # | Risk | Mitigation | Abandon the choice if… |
|---|---|---|---|
| R1 | An always-on VM or a forgotten cluster burns the credit (3.3) | budget alerts; VM stopped by rule; `instances list` / `batches list` in every post-run step | the alerts fire at $150 with M4 unfinished — then the remaining work moves back to local and the VM is deleted, not stopped |
| R2 | Spark 4.0.1 behaves differently from 4.1.2 in `TargetEncoder` metadata or `targetType` | the M0 parity test; both quirks are already pinned in `mllib.py` and its tests | the parity run lands outside fold noise and the cause is a 4.0.x defect — reopen the custom-image option with that as the reason |
| R3 | The Serverless batch cannot authenticate or lacks roles (runtime 3.0 uses end-user credentials by default) | look up the role set before M4; run as `nytaxi-pipeline` | never a reason to abandon Serverless; a reason to fix IAM |
| R4 | Dataset location vs bucket region mismatch makes external tables fail (2.1) | resolve before `terraform apply`; locations are immutable | — (a one-time check) |
| R5 | `COUNT(*)` in M3 ≠ 128,781,646 — the archive changed under the project (6.1) | fingerprint at M2; report the delta; regenerate samples from the cloud build | the delta is large enough to change the p99 caps — then the local backup is retired as the modeling input, and `paths.py` points at the new samples |
| R6 | OOF encoder has a silent leak | TDD tests in 5.3; the 12-row worst case; the 5.4 criterion cannot be inflated by training-half leakage | a leak is found *after* an MLlib arm has been reported — the row is withdrawn and the sweep re-run, exactly as the 2026-08-01 corrupt-odometer row was handled |
| R7 | sklearn §5c scope does not fit the Serverless driver (16 GB) or the laptop | category-dtype enabler first; probe `--only lightgbm --sample full`; short-lived VM as the fallback (§8 decision 3) | — |
| R8 | Submodule fix upstream stalls (owner's other repo, CI there) | it is a two-line YAML change; the pointer bump is the only change here | never — no workaround inside the submodule directory is acceptable |
| R9 | Terraform `apply` collides with something that already exists in the project | `plan` shows creates only, or `import` first (standing rule) | — |
| R10 | Trial clock (90 days) runs out before M5 | start the clock after M0; M1–M3 are days, not weeks; M4/M5 are hours of runtime each | the clock runs out with M3 done: the pipeline is proven, the §5c run moves to a paid few dollars — acceptable |
| R11 | Documentation URLs in this file rot (§0) | product names recorded as search terms | — |

---

## 8. Open decisions for the owner

Each with options and a recommendation. None is taken by this document.

1. **Provision, substitute, or archive** (audit item 5). *Options:* (a) provision under
   `$GCP_PROJECT_ID` on the trial — this plan; (b) local substitute (MinIO + DuckDB or
   similar) — proves nothing about the BigQuery/dbt/Airflow path and still leaves §5c with
   no target; (c) archive the GCP half as reference — the repository's portfolio claim
   becomes "designed", not "ran". **Recommend (a).** Settles D-006 as *keep*.

2. **Spark version on the cloud. TAKEN — (a), 2026-09-02.** *Options were:* (a)
   Serverless runtime 3.0, Spark 4.0.1, `requirements.txt` moved to 4.0.1 after the parity
   test; (b) custom image with 4.1.2; (c) GCE VM with the local install. The M0 parity test
   (2.4) measured 4.0.1 and 4.1.2 **bit-identical** on Phase 4b row 2 — every metric and
   every standard deviation equal to full float64 precision, per fold as well as in the
   mean. (a) is taken and `requirements.txt` is on 4.0.1. (b) needed a measured reason from
   that test and has none. (c) stays rejected on credit exposure. Recorded as **D-011**.

3. **Where the sklearn half of §5c runs.** *Options:* (a) the laptop after the
   category-dtype enabler, model by model (`--only`), `stacking` overnight; (b) a
   short-lived 8-vCPU / 32 GB GCE VM, deleted at the end (price UNVERIFIED — check first);
   (c) the Serverless driver, 16 GB, sklearn on the driver only — a misuse of the service.
   **Recommend (a) first**, with a single `--only lightgbm --sample full` probe as the
   decider; (b) if it OOMs or the encoder probe exceeds an hour.

4. **How far modeling moves to PySpark** (§5). *Options:* (a) the OOF encoder + MLlib arm
   of every comparison, sklearn champion until the memory boundary; (b) full port of
   `preprocess.py` / `evaluate.py` / the sweep, losing three model families and the
   multi-metric harness. **Recommend (a)**, gated on §5.4; record as the successor to D2.

5. **Terraform state.** *Options:* (a) local state, gitignored — one operator, one
   laptop; (b) GCS backend in the pipeline bucket under `tfstate/` — survives the laptop,
   costs a chicken-and-egg (the bucket must exist before the backend does). **Recommend
   (a) now**, (b) if a second machine ever runs Terraform. The three tracked files leave
   git either way.

6. **When to start the 90-day clock.** *Options:* sign up now, or after M0. **Recommend
   after M0** — the parity test, the submodule fix and the Terraform rewrite spend no
   cloud time and should not spend trial days.

7. **VM size and the `e2-micro` question.** The Always Free `e2-micro` (1 vCPU shared,
   1 GB) is ASSUMED unable to run the five-container compose stack. *Options:* try it
   first because it is free, or start at `e2-standard-2`. **Recommend `e2-standard-2`**
   and stop it by rule; the free tier is a bonus, not a design constraint.

8. **Ingest window.** *Options:* keep 2015-01–2016-12 for the migration; extend later per
   §6.2. **Recommend keep** — the migration's job is reproducing 128,781,646 rows, and
   every later-year risk in §6.2 is a separate decision with its own gate.

---

## Status

- [ ] Plan reviewed by Sagar
- [ ] Decisions 1–8 taken; D-006 closed; D2 successor entry added to `notes/decisions.md`
- [x] M0 — hygiene (tests first; tfstate out of git; `main.tf` rewrite; submodule fix
      upstream + pointer bump; Spark 4.0.1 parity test). **Complete 2026-09-02.** The
      submodule fix landed last, as upstream `305868f`; the pointer moved off `d11219d`,
      both `xfail(strict=True)` guards reported `XPASS(strict)` on the bump, and the marker
      was deleted. Gate: 292 passed, 1 skipped, 0 xfailed, the 9 integration failures
      unchanged — they need M1's provisioned project.
- [ ] M1 — provision via Terraform; budget alerts first
- [ ] M2 — DAGs proven from the laptop; archive fingerprint recorded
- [ ] M3 — `dbt_prod` rebuilt; `COUNT(*) = 128,781,646` checked; 0 integration failures
- [ ] M4 — Serverless smoke inside fold noise; prep from BigQuery matches `prep_stats.json`
- [ ] §5.3 OOF encoder (TDD) and §5.4 acceptance run — result recorded in the modeling plan §5b
- [ ] §5c run scoped per decision 3
- [ ] M5 — Airflow VM on ADC, one DAG end to end, VM stopped
- [ ] M6 — documents reconciled; audit items 5, 6, 8 checked off
