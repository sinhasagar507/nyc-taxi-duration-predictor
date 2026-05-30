"""
Unit tests for DAG configuration correctness — no Airflow installation required.
Checks env var usage, absence of stale hardcoded IDs, and DAG chaining wiring.
"""

import pytest
from pathlib import Path

DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"

STALE_IDS = [
    "dtc-de-course-466501",
    "dtc-de-course-440404",
    "dtc-de-project_1",
    "dbt_production",
]

INGEST_DAGS = [
    "nyc_taxi_gcs_yellow_dag.py",
    "nyc_taxi_gcs_green_dag.py",
    "nyc_climate_gcs_dag.py",
    "nyc_taxi_zone_gcs_dag.py",
]

EXTERNAL_TABLE_DAGS = [
    "yellow_taxi_external_table.py",
    "green_taxi_external_table.py",
    "climate_data_external_table.py",
    "nyc_taxi_zone_external_table.py",
]

# Maps each ingest DAG to the downstream DAG it must trigger.
INGEST_TO_DOWNSTREAM = {
    "nyc_taxi_gcs_yellow_dag.py": "create_external_table_yellow_taxi",
    "nyc_taxi_gcs_green_dag.py": "create_external_table_green_taxi",
    "nyc_climate_gcs_dag.py": "create_external_table_climate_data",
}


class TestNoStaleIDs:
    @pytest.mark.parametrize("dag_file", INGEST_DAGS + EXTERNAL_TABLE_DAGS)
    def test_dag_contains_no_stale_project_or_bucket_ids(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        found = [sid for sid in STALE_IDS if sid in content]
        assert not found, (
            f"{dag_file} contains stale IDs that must be removed: {found}"
        )


class TestEnvVarUsage:
    @pytest.mark.parametrize("dag_file", INGEST_DAGS + EXTERNAL_TABLE_DAGS)
    def test_dag_reads_project_from_env(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        assert 'os.environ' in content, (
            f"{dag_file} does not read from os.environ — project/bucket must not be hardcoded"
        )

    def test_dbt_dag_uses_dbt_core_not_dbt_cloud(self):
        content = (DAGS_DIR / "dbt_run_dag.py").read_text()
        assert "dbt build" in content or "BashOperator" in content, (
            "dbt_run_dag.py should run dbt via BashOperator (dbt Core), not dbt Cloud"
        )
        assert "DbtCloudRunJobOperator" not in content, (
            "dbt_run_dag.py must not use DbtCloudRunJobOperator (project uses dbt Core)"
        )


class TestDAGChaining:
    @pytest.mark.parametrize("ingest_dag,downstream_id", INGEST_TO_DOWNSTREAM.items())
    def test_ingest_dag_triggers_external_table_dag(self, ingest_dag, downstream_id):
        content = (DAGS_DIR / ingest_dag).read_text()
        assert downstream_id in content, (
            f"{ingest_dag} must trigger downstream DAG '{downstream_id}' "
            f"via TriggerDagRunOperator"
        )

    @pytest.mark.parametrize("ingest_dag", INGEST_TO_DOWNSTREAM)
    def test_ingest_dag_uses_trigger_dag_run_operator(self, ingest_dag):
        content = (DAGS_DIR / ingest_dag).read_text()
        assert "TriggerDagRunOperator" in content, (
            f"{ingest_dag} must use TriggerDagRunOperator to chain to the external-table DAG"
        )
