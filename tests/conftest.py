import os
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

GCP_PROJECT = "dtc-de-project-492321"
GCS_BUCKET = "primary-data-dtc"
BQ_TAXI_DATASET = "nyc_taxi_data"
BQ_CLIMATE_DATASET = "nyc_climate_data"
BQ_DBT_PROD_DATASET = "dbt_prod"
CREDS_PATH = PROJECT_ROOT / "secrets" / "dtc-de-project-492321-970e67a252d8.json"
DAGS_DIR = PROJECT_ROOT / "airflow" / "dags"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "ny_taxi_analytics"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires GOOGLE_APPLICATION_CREDENTIALS; auto-skipped if absent",
    )
    config.addinivalue_line(
        "markers",
        "e2e: requires running Airflow stack (docker compose up)",
    )


def pytest_collection_modifyitems(config, items):
    if CREDS_PATH.exists():
        # Always prefer our project's known-good credentials over any ambient env var.
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDS_PATH)
        creds_available = True
    else:
        env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        creds_available = bool(env_path) and Path(env_path).exists()

    if not creds_available:
        skip = pytest.mark.skip(reason="GCP credentials not found (secrets/ absent and GOOGLE_APPLICATION_CREDENTIALS unset or points to missing file)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
