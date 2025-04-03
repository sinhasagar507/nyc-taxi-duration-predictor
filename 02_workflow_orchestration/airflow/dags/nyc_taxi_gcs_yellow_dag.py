import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
TAXI_DATA_DIRECTORY = "nyc_taxi_trips" # folder path within the docker taxi data directory
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/") # the root path within the DOCKER environment
TRIPDATA_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data" # URL of the TAXI data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet
YELLOW_TRIP_SOURCE_DATASET_FILE = TRIPDATA_URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet" # Dataset URL at the source
YELLOW_TRIP_TARGET_DATASET_FILE = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet" # Dataset URL at the destination and bigquery

TAXI_BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET", "nyc_tlc_trips")
YELLOW_TAXI_BIGQUERY_TABLE_ID = os.environ.get ("YELLOW_TRIP_BIGQUERY_DATASET", "yellow_taxi_data")


# DATASET_FILE = "yellow_tripdata_2015-01.parquet"
# DATASET_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{DATASET_FILE}"
# PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def remove_local_file(file_path):
    """
    Deletes the file from the local Airflow directory after upload.
    """
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Deleted local file: {file_path}")
    else:
        logging.warning(f"File not found: {file_path}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="nyc_taxi_data_ingestion_dag",
    default_args=default_args,
    start_date=datetime(2016, 1, 1), # Start from Jan 1 2015
    end_date=datetime(2020, 12, 31), # End on 30th March 2015
    schedule_interval='0 6 1 * *',   # 6 AM on the 1st of every month
    catchup=True,                    # Process historical data
    max_active_runs=3,               # Limit parallel runs
    tags=['dtc-de'],
) as dag:
    
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {YELLOW_TRIP_SOURCE_DATASET_FILE} > {PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/nyc_taxi_data/{YELLOW_TRIP_TARGET_DATASET_FILE}",
            "local_file": f"{PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
        },
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": TAXI_BIGQUERY_DATASET_ID,
    #             "tableId": YELLOW_TAXI_BIGQUERY_TABLE_ID,
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": ["gs://{}/raw/nyc_taxi_data/yellow_tripdata_{{{{ execution_date.strftime('%Y-%m') }}}}.parquet".format(BUCKET)],
    #         },
    #     },
    # )
    
    # cleanup_local_file_task = PythonOperator(
    #     task_id="cleanup_local_file_task",
    #     python_callable=remove_local_file,
    #     op_kwargs={
    #         "file_path": f"{PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
    #     },
    # )
    
    download_dataset_task >> local_to_gcs_task
    
    
    

