"""
Build the BigQuery marts with **dbt Core**, run locally inside Airflow.

The dbt project lives in the `dbt/ny_taxi_analytics` git submodule (mounted into the
container at /opt/airflow/dbt/ny_taxi_analytics). dbt reads sources from the BigQuery
external tables (nyc_taxi_data.*, nyc_climate_data.*) and writes the staging views +
core tables into the dbt_prod dataset.

dbt is installed in an isolated virtualenv (/opt/dbt-venv, built in the Airflow image's
dockerfile) so its dependencies never collide with Airflow's. The BigQuery service
account is resolved from GOOGLE_APPLICATION_CREDENTIALS (already set + mounted in the
container); profiles.yml reads that env var, so no key path is hardcoded.

Run this after the ingestion + external-table DAGs have refreshed the source tables.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/ny_taxi_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"

# `dbt deps` installs packages (dbt_utils, codegen); `dbt build` runs seeds + models +
# tests in DAG order. Artifacts (target/, dbt_packages/) are written under the project
# dir, which dbt's own .gitignore excludes.
dbt_build_command = (
    f"cd {DBT_PROJECT_DIR} && "
    f"{DBT_BIN} deps && "
    f"{DBT_BIN} build --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"
)

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="dbt_build_marts",
    default_args=default_args,
    schedule_interval=None,   # run on demand, after external tables are refreshed
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "dbt-core"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_build_command,
    )

    dbt_build
