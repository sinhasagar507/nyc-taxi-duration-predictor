terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
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

# The data lake. Regional us-central1 (var.region), which is the Always Free
# condition for the 5 GB-month allowance. Prefix layout (notes/gcp-reference.md):
#   nyc_taxi_data/  nyc_climate_data/  taxi_lookup_data/  ml/samples/  ml/models/
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.region
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

# UNVERIFIED (plan 2.1): whether a US multi-region dataset can define an external
# table over a us-central1 bucket, or whether the datasets must be us-central1 to
# match. var.location is "US", which agrees with the `location: US` in
# dbt/profiles.yml. Settle this before the first apply — a dataset's location
# cannot be changed afterwards.
resource "google_bigquery_dataset" "pipeline" {
  for_each = local.bigquery_datasets

  dataset_id = each.value
  location   = var.location

  # These datasets already exist on the live project. Terraform must `import`
  # them before any apply (CLAUDE.md rule); deletion is never the intent here.
  delete_contents_on_destroy = false
}
