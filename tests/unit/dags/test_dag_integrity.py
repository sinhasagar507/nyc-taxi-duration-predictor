"""
Unit tests for DAG file integrity — no Airflow installation required.
Checks that every expected DAG file exists and is syntactically valid Python.
"""

import ast
from pathlib import Path
import pytest

DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"

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

ORCHESTRATION_DAGS = [
    "dbt_run_dag.py",
]

ALL_DAGS = INGEST_DAGS + EXTERNAL_TABLE_DAGS + ORCHESTRATION_DAGS


class TestDAGFilesExist:
    def test_all_dag_files_present(self):
        missing = [f for f in ALL_DAGS if not (DAGS_DIR / f).exists()]
        assert not missing, f"Missing DAG files: {missing}"

    def test_dags_directory_contains_no_unexpected_python_files(self):
        actual = {p.name for p in DAGS_DIR.glob("*.py") if p.name != "__init__.py"}
        expected = set(ALL_DAGS)
        unexpected = actual - expected
        assert not unexpected, (
            f"Unexpected files in dags/: {unexpected}\n"
            "Add them to ALL_DAGS in this test if they are intentional."
        )


class TestDAGSyntax:
    @pytest.mark.parametrize("dag_file", ALL_DAGS)
    def test_dag_file_is_valid_python(self, dag_file):
        path = DAGS_DIR / dag_file
        try:
            ast.parse(path.read_text())
        except SyntaxError as e:
            pytest.fail(f"{dag_file} has a syntax error: {e}")

    @pytest.mark.parametrize("dag_file", ALL_DAGS)
    def test_dag_file_defines_a_dag(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        assert "with DAG(" in content or "DAG(" in content, (
            f"{dag_file} does not define a DAG object"
        )
