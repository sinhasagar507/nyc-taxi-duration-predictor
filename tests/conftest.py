import os
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# Single source of truth for project/bucket = environment (see repo-root .env).
# Defaults keep the suite working against the current live project when no env is set.
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "dtc-de-project-492321")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "primary-data-dtc")
BQ_TAXI_DATASET = "nyc_taxi_data"
BQ_CLIMATE_DATASET = "nyc_climate_data"
BQ_DBT_PROD_DATASET = "dbt_prod"

# Decoupled credential path: the key lives at a stable, project-agnostic filename so a
# GCP account swap is a one-file drop (replace this file, no code change). Kept as a
# fixed project path so the suite still prefers this key over any stale ambient env var.
CREDS_PATH = PROJECT_ROOT / "secrets" / "gcp-credentials.json"
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
