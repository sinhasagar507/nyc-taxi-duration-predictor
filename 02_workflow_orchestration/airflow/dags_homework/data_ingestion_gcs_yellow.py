import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
TRIPDATA_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_TRIP_SOURCE_DATASET_FILE = TRIPDATA_URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
YELLOW_TRIP_TARGET_DATASET_FILE = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
YELLOW_TRIP_BIGQUERY_DATASET = os.environ.get("YELLOW_TRIP_BIGQUERY_DATASET", "yellow_trips_data")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "demo_dataset")


# Function to format the source file to parquet if not already in the format
def format_to_parquet(src_file):
    """
    Creates an empty DataFrame, loads the source file into it (CSV or Parquet),
    and saves it as a valid Parquet file.
    
    :param src_file: Path to the source file (CSV or Parquet).
    """
    try:
        # Create an empty DataFrame
        logging.info("Creating an empty DataFrame.")
        empty_df = pd.DataFrame()

        # Load source file into the DataFrame
        if src_file.endswith('.csv'):
            logging.info(f"Loading CSV file: {src_file}")
            df = pd.read_csv(src_file)  # Load CSV file
        elif src_file.endswith('.parquet'):
            logging.info(f"Loading Parquet file: {src_file}")
            df = pd.read_parquet(src_file, engine='pyarrow')  # Load Parquet file
        else:
            logging.error("Unsupported file format. Only CSV and Parquet are accepted.")
            return

        # Combine with the empty DataFrame to ensure consistency
        final_df = pd.concat([empty_df, df])

        # Save as a Parquet file
        output_file = src_file.rsplit('.', 1)[0] + ".parquet"  # Replace extension with .parquet
        logging.info(f"Saving file as Parquet: {output_file}")
        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, output_file)

        logging.info(f"File successfully saved as Parquet: {output_file}")
    except Exception as e:
        logging.error(f"Error processing file: {src_file}. Error: {e}")

    
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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id='yellow_taxi_data_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),        # Start from Jan 1 2023
    end_date=datetime(2024, 7, 30),
    schedule_interval='0 6 1 * *',          # 6 AM on the 1st of every month
    catchup=True,                           # Process historical data
    max_active_runs=3,                      # Limit parallel runs
) as dag:
    


    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {YELLOW_TRIP_SOURCE_DATASET_FILE} > {PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}"
    )
    
    convert_to_parquet_task = PythonOperator(
        task_id = "convert_to_parquet_task", 
        python_callable = format_to_parquet, 
        op_kwargs={
            "src_file": f"{PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
        },
    )
    
    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"hw/yellow_taxi/{{{{ execution_date.strftime('%Y') }}}}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
            "local_file": f"{PATH_TO_LOCAL_HOME}/{YELLOW_TRIP_TARGET_DATASET_FILE}",
        },
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/hw/{{{{ execution_date.strftime('%Y') }}}}/{YELLOW_TRIP_TARGET_DATASET_FILE}"],
    #         },
    #     },
    # )

    download_dataset_task >> convert_to_parquet_task >> local_to_gcs_task 