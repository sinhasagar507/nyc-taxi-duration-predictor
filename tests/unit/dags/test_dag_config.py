"""
Unit tests for DAG configuration correctness — no Airflow installation required.
Checks env var usage, absence of hardcoded IDs, and DAG chaining wiring.

**Generalised 2026-09-02.** This used to hold a literal list of stale project IDs. A
literal list only catches the mistakes already made; it said nothing about the *current*
project ID being hardcoded, which is the same defect wearing a newer name. The guard now
matches the shape of any project ID in this account's naming family, so it catches the
next one too — and it carries no account identifier in the test file.

A pattern that matches nothing passes forever while protecting nothing, so
`TestTheGuardStillBites` proves each pattern still fires on a synthetic example.
"""

import re
import pytest
from pathlib import Path

DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"

# Any hardcoded project ID from this account's naming family, current or stale. The DAGs
# read GCP_PROJECT_ID from os.environ (see TestEnvVarUsage), so no literal belongs in
# one — a correct-looking literal is worse than a stale one, because it works until the
# project changes and then fails somewhere far away.
PROJECT_ID_PATTERN = re.compile(r"dtc-de-[a-z0-9][a-z0-9_-]*")

# Not an ID, a naming rule: the dbt prod dataset is `dbt_prod`. `dbt_production` is a
# recurring typo that points at a dataset which does not exist, and BigQuery reports that
# as a missing table rather than as a missing dataset.
WRONG_DATASET = "dbt_production"

# Synthetic, not real. Used only to prove the pattern still bites.
SYNTHETIC_PROJECT_ID = "dtc-de-example-000000"

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


class TestNoHardcodedIDs:
    @pytest.mark.parametrize("dag_file", INGEST_DAGS + EXTERNAL_TABLE_DAGS)
    def test_dag_contains_no_hardcoded_project_id(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        found = sorted(set(PROJECT_ID_PATTERN.findall(content)))
        assert not found, (
            f"{dag_file} hardcodes a project ID: {found}. Read GCP_PROJECT_ID from "
            "os.environ instead — docker-compose.yaml is the single source of truth."
        )

    @pytest.mark.parametrize("dag_file", INGEST_DAGS + EXTERNAL_TABLE_DAGS)
    def test_dag_does_not_use_the_wrong_dbt_dataset(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        assert WRONG_DATASET not in content, (
            f"{dag_file} references '{WRONG_DATASET}'. The dbt prod dataset is "
            "'dbt_prod'; the longer name resolves to nothing."
        )


class TestTheGuardStillBites:
    """Positive controls. Without these, a broken pattern passes silently forever."""

    def test_project_pattern_catches_a_hardcoded_id(self):
        found = PROJECT_ID_PATTERN.findall(f"PROJECT_ID = '{SYNTHETIC_PROJECT_ID}'")
        assert found == [SYNTHETIC_PROJECT_ID]

    def test_project_pattern_ignores_ordinary_dag_text(self):
        """It must not fire on the words a normal DAG is made of."""
        ordinary = "PROJECT_ID = os.environ.get('GCP_PROJECT_ID')  # create_external_table"
        assert not PROJECT_ID_PATTERN.search(ordinary)


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


class TestParametrizedDateRanges:
    """Ingest backfill window must come from env vars, not hardcoded datetimes.

    Single source of truth is docker-compose.yaml (INGEST_START_DATE /
    INGEST_END_DATE); DAGs read them via os.environ with the 2015-2016
    window as the fallback default.
    """

    CATCHUP_INGEST_DAGS = [
        "nyc_taxi_gcs_yellow_dag.py",
        "nyc_taxi_gcs_green_dag.py",
    ]

    @pytest.mark.parametrize("dag_file", CATCHUP_INGEST_DAGS)
    def test_ingest_dag_has_no_hardcoded_date_range(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        hardcoded = [
            lit for lit in ("datetime(2015", "datetime(2016") if lit in content
        ]
        assert not hardcoded, (
            f"{dag_file} hardcodes the backfill window ({hardcoded}); "
            f"read INGEST_START_DATE / INGEST_END_DATE from the environment instead"
        )

    @pytest.mark.parametrize("dag_file", CATCHUP_INGEST_DAGS)
    def test_ingest_dag_reads_date_range_from_env(self, dag_file):
        content = (DAGS_DIR / dag_file).read_text()
        for var in ("INGEST_START_DATE", "INGEST_END_DATE"):
            assert var in content, (
                f"{dag_file} must read the backfill window from the {var} env var"
            )

    def test_docker_compose_defines_ingest_date_range(self):
        compose = (DAGS_DIR.parents[0] / "docker-compose.yaml").read_text()
        for var in ("INGEST_START_DATE", "INGEST_END_DATE"):
            assert var in compose, (
                f"airflow/docker-compose.yaml must define {var} (single source of truth "
                f"for the ingest backfill window)"
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
