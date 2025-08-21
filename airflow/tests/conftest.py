"""
Pytest configuration and fixtures for Airflow DAG testing.

This file contains shared fixtures and configuration that can be used
across all test modules.
"""

import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
try:
    from airflow.models import DagBag, TaskInstance
    from airflow.utils.state import State
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Mock classes for when Airflow is not available
    AIRFLOW_AVAILABLE = False
    
    class DagBag:
        def __init__(self, *args, **kwargs):
            self.import_errors = {}
            self.dags = {}
        
        def get_dag(self, dag_id):
            return None
    
    class TaskInstance:
        pass
    
    class State:
        SUCCESS = "success"


@pytest.fixture(scope="session")
def dagbag():
    """
    Fixture to load all DAGs for testing.
    This runs once per test session to improve performance.
    """
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture
def mock_gcp_environment():
    """
    Mock GCP environment variables for testing.
    """
    with patch.dict(os.environ, {
        'GCP_PROJECT_ID': 'test-project-id',
        'GCP_GCS_BUCKET': 'test-bucket',
        'BIGQUERY_DATASET': 'test_dataset',
        'AIRFLOW_HOME': '/tmp/airflow'
    }):
        yield


@pytest.fixture
def sample_execution_date():
    """
    Provide a consistent execution date for testing.
    """
    return datetime(2024, 1, 15, 12, 0, 0)


@pytest.fixture
def mock_gcs_client():
    """
    Mock Google Cloud Storage client for testing file operations.
    """
    with patch('google.cloud.storage.Client') as mock_client:
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        yield mock_client


@pytest.fixture
def mock_bigquery_client():
    """
    Mock BigQuery client for testing database operations.
    """
    with patch('google.cloud.bigquery.Client') as mock_client:
        mock_job = Mock()
        mock_client.return_value.query.return_value = mock_job
        yield mock_client


@pytest.fixture
def sample_taxi_data():
    """
    Sample taxi data for testing data processing functions.
    """
    return {
        'pickup_datetime': '2024-01-15 10:30:00',
        'dropoff_datetime': '2024-01-15 10:45:00',
        'pickup_locationid': 161,
        'dropoff_locationid': 236,
        'trip_distance': 2.5,
        'fare_amount': 12.50
    }


@pytest.fixture
def mock_subprocess():
    """
    Mock subprocess calls for testing download operations.
    """
    with patch('subprocess.run') as mock_run:
        mock_run.return_value.returncode = 0
        yield mock_run


class TestTaskInstance:
    """
    Helper class to create TaskInstance objects for testing.
    """
    
    @staticmethod
    def create_task_instance(dag, task_id, execution_date, state=State.SUCCESS):
        """
        Create a TaskInstance for testing.
        """
        task = dag.get_task(task_id)
        ti = TaskInstance(task=task, execution_date=execution_date)
        ti.state = state
        return ti


# Test data constants
TEST_DATA_PATHS = {
    'yellow_taxi_sample': 'tests/data/yellow_tripdata_sample.parquet',
    'green_taxi_sample': 'tests/data/green_tripdata_sample.parquet',
    'climate_sample': 'tests/data/weather_sample.csv'
}

# Mock URLs for testing
MOCK_URLS = {
    'taxi_data': 'https://test-url.com/yellow_tripdata_2024-01.parquet',
    'climate_data': 'https://test-url.com/weather_data.csv'
}
