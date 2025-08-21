import os
import logging
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# === Configuration ===
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
TRIPDATA_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET", "nyc_tlc_trips")
YELLOW_TAXI_BIGQUERY_TABLE_ID = os.environ.get("YELLOW_TRIP_BIGQUERY_DATASET", "yellow_taxi_data")


# === Task: Download File using curl -L ===
def download_file(execution_date, **context):
    file_name = f"yellow_tripdata_{execution_date.strftime('%Y-%m')}.parquet"
    url = f"{TRIPDATA_URL_PREFIX}/{file_name}"
    local_path = os.path.join(PATH_TO_LOCAL_HOME, file_name)

    logging.info(f"📥 Downloading using curl -L: {url}")
    try:
        subprocess.run(
            ["curl", "-L", "-o", local_path, url],
            check=True
        )
        logging.info(f"✅ Downloaded to: {local_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ curl failed with error: {e}")
        raise


# === Task: Upload to GCS ===
def upload_to_gcs(bucket, execution_date, **context):
    file_name = f"yellow_tripdata_{execution_date.strftime('%Y-%m')}.parquet"
    local_file = os.path.join(PATH_TO_LOCAL_HOME, file_name)
    object_name = f"nyc_taxi_data/yellow_taxi_data/{file_name}"

    # GCS Upload Workaround
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(object_name)
    blob.upload_from_filename(local_file)

    logging.info(f"📤 Uploaded {file_name} to gs://{bucket}/{object_name}")


# === Task: Remove local file ===
def remove_local_file(execution_date, **context):
    file_name = f"yellow_tripdata_{execution_date.strftime('%Y-%m')}.parquet"
    file_path = os.path.join(PATH_TO_LOCAL_HOME, file_name)

    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"🗑️ Deleted local file: {file_path}")
    else:
        logging.warning(f"⚠️ File not found: {file_path}")


# === DAG Definition ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_data_ingestion_dag",
    default_args=default_args,
    start_date=datetime(2012, 1, 1),
    end_date=datetime(2012, 1, 31),
    schedule_interval='0 6 1 * *',
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_file,
        provide_context=True
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={"bucket": BUCKET},
        provide_context=True
    )

    cleanup_local_file_task = PythonOperator(
        task_id="cleanup_local_file_task",
        python_callable=remove_local_file,
        provide_context=True
    )

    # Sequential flow: Download → Upload → Cleanup
    download_dataset_task >> local_to_gcs_task >> cleanup_local_file_task
