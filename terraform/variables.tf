variable "credentials" {
  description = "Path to the GCP service-account key (stable, project-agnostic filename)"
  default     = "../secrets/gcp-credentials.json"
}


variable "project" {
  description = "GCP project ID (override via TF_VAR_project / GCP_PROJECT_ID on account swap)"
  default     = "dtc-de-project-506916"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "nyc_tlc_trips"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "primary-data-dtc-506916"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
