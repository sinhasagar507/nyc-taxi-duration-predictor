import os
import logging
from datetime import datetime

import pyarrow.csv as pc
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# Local file imports 
from ingest_script import ingest_callable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parquet_to_csv(parquet_file, csv_file):
    """
    Converts a Parquet file to CSV format and logs the outcome.
    
    :param parquet_file: Path to the input Parquet file.
    :param csv_file: Path to the output CSV file.
    """
    try:
        # Read the Parquet file
        table = pq.read_table(parquet_file)
        
        # Write the table to CSV
        pc.write_csv(table, csv_file)
        
        # Log success
        logging.info(f"Successfully converted Parquet file '{parquet_file}' to CSV file '{csv_file}'")
        
    except Exception as e:
        # Log failure
        logging.error(f"Failed to convert Parquet file '{parquet_file}' to CSV file. Error: {str(e)}")

# Define the DAG with a start_date and other required parameters
local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval="0 6 20 * *",  # Schedule: 6 AM on the 20th day of every month
    start_date=datetime(2024, 12, 20),  # Replace with an appropriate start date
    catchup=False,  # Optional: Prevent backfilling for past dates
)

# Base directories
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILENAME = "ny_taxi_yellow_2024.parquet"
CSV_FILENAME = PARQUET_FILENAME.replace("parquet", "csv")
PARQUET_FILE_PATH = os.path.join(AIRFLOW_HOME, PARQUET_FILENAME)
CSV_FILEPATH = os.path.join(AIRFLOW_HOME, CSV_FILENAME)

# URL and dataset details
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
DATASET = "yellow_tripdata_2021-02.csv.gz"
URL_TEMPLATE = URL_PREFIX + DATASET
OUTPUT_FILE_TEMPLATE = os.path.join(AIRFLOW_HOME, DATASET)

# Postgres database connection environment variables
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

with local_workflow:
    curl_task = BashOperator(
        task_id="curl_command",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    )
    
    # parquet_to_csv_task = PythonOperator(
    #     task_id="parquet_csv_convert", 
    #     python_callable=parquet_to_csv, 
    #     op_kwargs={
    #         "parquet_file": PARQUET_FILE_PATH, 
    #         "csv_file": CSV_FILEPATH,
    #     },  
    # )

    # curl_task = BashOperator(
    #     task_id="ingest",
    #     bash_command='ls ${AIRFLOW_HOME}',
    # )
    
    ingest_task = PythonOperator(
        task_id="ingest_data_postgres", 
        python_callable=ingest_callable, 
        op_kwargs={
            "user": PG_USER, 
            "password": PG_PASSWORD, 
            "host": PG_HOST, 
            "port": PG_PORT, 
            "db": PG_DATABASE,
            "table_name": "???", # Just testing for now, change it soon
            "csv_file": OUTPUT_FILE_TEMPLATE,
        }
    )

    curl_task >> ingest_task
