import os
import logging
from datetime import datetime

import pyarrow.csv as pc
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

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
    schedule_interval="0 6 20 * *",  # Schedule: 6 AM on the 2nd day of every month
    start_date=datetime(2024, 12, 20),  # Replace with an appropriate start date
    catchup=False,  # Optional: Prevent backfilling for past dates
)

DATASET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILENAME = "ny_taxi_yellow_2024.parquet"
CSV_FILENAME = PARQUET_FILENAME.replace("parquet", "csv")
PARQUET_FILE_PATH = f"{AIRFLOW_HOME}/{PARQUET_FILENAME}"
CSV_FILEPATH = f"{AIRFLOW_HOME}/{CSV_FILENAME}"

with local_workflow:
    curl_task = BashOperator(
        task_id="curl_command",
        bash_command=f'curl -sSL {DATASET_URL} > {PARQUET_FILE_PATH}',
    )
    
    parquet_to_csv_task = PythonOperator(
        task_id="parquet_csv_convert", 
        python_callable=parquet_to_csv, 
        op_kwargs={
            "parquet_file": PARQUET_FILE_PATH, 
            "csv_file": CSV_FILEPATH,
        },  
    )

    # ingest_task = BashOperator(
    #     task_id="ingest",
    #     bash_command='echo "Hello World"',
    # )

    curl_task >> parquet_to_csv_task
