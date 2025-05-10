from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Configuration
PROJECT_ID = 'dtc-de-course-457315'
DATASET_ID = 'nyc_taxi_data'
# We'll name the external table differently to avoid confusion with any native table loads.
TABLE_ID = 'taxi_zone_external_table'
BUCKET_NAME = 'dtc-de-project'
SOURCE_FOLDER = 'nyc_taxi_data/taxi_lookup_data/'

# SQL query to create or replace an external table referencing your Parquet files in GCS.
sql_query = f"""
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{BUCKET_NAME}/{SOURCE_FOLDER}*.csv']
);
"""

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

with models.DAG(
    dag_id='create_external_table_taxi_zone',
    default_args=default_args,
    schedule_interval=None,   # Run on demand
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'external', 'gcs'],
) as dag:

    create_external_table = BigQueryInsertJobOperator(
        task_id='create_external_table',
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
            }
        },
        location='US',  # Change if your BigQuery dataset is in a different location
        gcp_conn_id='google_cloud_default',
    )

    create_external_table