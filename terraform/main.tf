terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }

  # Fresh state, deliberately NOT the default `terraform.tfstate`. The legacy
  # state files in this directory (serial 1-3, 2025) describe a dead project,
  # `dtc-de-course-457315`, and two resources this config no longer declares
  # (`demo_dataset`, `demo-bucket`). Reading them would make `plan` propose
  # destroys. The name still matches the `terraform/*.tfstate*` gitignore rule,
  # so no state is ever committed.
  backend "local" {
    path = "pipeline.tfstate"
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

locals {
  # The five datasets the pipeline actually reads or writes.
  #   nyc_taxi_data    — external tables: yellow, green, taxi zone lookup
  #   nyc_climate_data — external table: climate
  #   dbt_prod/dev/ci  — the three dbt targets in dbt/profiles.yml
  # Names are verified against airflow/dags/*_external_table.py (DATASET_ID),
  # dbt/ny_taxi_analytics/models/staging/schema_*.yml (source schema:) and
  # dbt/profiles.yml (dataset:). Do not rename either side alone.
  bigquery_datasets = toset([
    "nyc_taxi_data",
    "nyc_climate_data",
    "dbt_prod",
    "dbt_dev",
    "dbt_ci",
  ])
}

# The data lake. Location is var.gcs_bucket_location, NOT var.region.
# Measured 2026-09-02: the live bucket `primary-data-dtc-506916` is `US`
# multi-region. A bucket's location is immutable, so binding this to
# var.region ("us-central1") would make the plan after `terraform import`
# a REPLACE, destroying the 7.05 GiB already stored. The cost of staying on
# the multi-region: it forfeits the Always Free 5 GB-month allowance, which
# is us-central1-only. That is ~$0.19/month at the current 7.05 GiB.
# A new bucket in a new project should use "us-central1" instead.
# Prefix layout (notes/gcp-reference.md):
#   nyc_taxi_data/  nyc_climate_data/  taxi_lookup_data/  ml/samples/  ml/models/
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.gcs_bucket_location
  storage_class = var.gcs_storage_class

  # The bucket holds the only copy of the ingested parquet. Never let a destroy
  # take the objects with it.
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# VERIFIED 2026-09-02 (plan 2.1, docs.cloud.google.com/bigquery/docs/locations):
# a `US` multi-region dataset CAN define and query an external table over a
# us-central1 bucket, with no data-transfer charge. So var.location = "US" is
# correct and agrees with the `location: US` in all three dbt/profiles.yml
# targets. A dataset's location cannot be changed after creation.
resource "google_bigquery_dataset" "pipeline" {
  for_each = local.bigquery_datasets

  dataset_id = each.value
  location   = var.location

  # These datasets already exist on the live project. Terraform must `import`
  # them before any apply (CLAUDE.md rule); deletion is never the intent here.
  delete_contents_on_destroy = false
}
