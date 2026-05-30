"""
Integration tests — BigQuery smoke tests.
Verifies datasets exist, external tables are queryable, and dbt prod tables are populated.
Requires GOOGLE_APPLICATION_CREDENTIALS (auto-skipped if absent).
"""

import pytest
from google.cloud import bigquery

from tests.conftest import GCP_PROJECT, BQ_TAXI_DATASET, BQ_CLIMATE_DATASET, BQ_DBT_PROD_DATASET

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def bq():
    return bigquery.Client(project=GCP_PROJECT)


def _row_count(client, table_ref):
    result = client.query(f"SELECT COUNT(*) AS n FROM `{table_ref}`").result()
    return next(result).n


class TestDatasetsExist:
    def test_nyc_taxi_data_dataset_exists(self, bq):
        ids = {d.dataset_id for d in bq.list_datasets()}
        assert BQ_TAXI_DATASET in ids, f"Dataset '{BQ_TAXI_DATASET}' not found in project"

    def test_nyc_climate_data_dataset_exists(self, bq):
        ids = {d.dataset_id for d in bq.list_datasets()}
        assert BQ_CLIMATE_DATASET in ids, f"Dataset '{BQ_CLIMATE_DATASET}' not found in project"

    def test_dbt_prod_dataset_exists(self, bq):
        ids = {d.dataset_id for d in bq.list_datasets()}
        assert BQ_DBT_PROD_DATASET in ids, f"Dataset '{BQ_DBT_PROD_DATASET}' not found in project"


class TestExternalTablesQueryable:
    def test_yellow_external_table_has_rows(self, bq):
        count = _row_count(bq, f"{GCP_PROJECT}.{BQ_TAXI_DATASET}.yellow_taxi_external_table")
        assert count > 0, "yellow_taxi_external_table is empty"

    def test_green_external_table_has_rows(self, bq):
        count = _row_count(bq, f"{GCP_PROJECT}.{BQ_TAXI_DATASET}.green_taxi_external_table")
        assert count > 0, "green_taxi_external_table is empty"

    def test_climate_external_table_has_rows(self, bq):
        count = _row_count(bq, f"{GCP_PROJECT}.{BQ_CLIMATE_DATASET}.climate_external_table")
        assert count > 0, "climate_external_table is empty"


class TestDbtProdTables:
    def test_fact_trips_is_populated(self, bq):
        count = _row_count(bq, f"{GCP_PROJECT}.{BQ_DBT_PROD_DATASET}.fact_trips")
        assert count > 0, "dbt_prod.fact_trips is empty — dbt build may not have run"

    def test_dim_zones_exists(self, bq):
        table_ids = {t.table_id for t in bq.list_tables(f"{GCP_PROJECT}.{BQ_DBT_PROD_DATASET}")}
        assert "dim_zones" in table_ids, "dbt_prod.dim_zones not found"

    def test_dim_monthly_zones_revenue_exists(self, bq):
        table_ids = {t.table_id for t in bq.list_tables(f"{GCP_PROJECT}.{BQ_DBT_PROD_DATASET}")}
        assert "dim_monthly_zones_revenue" in table_ids, "dbt_prod.dim_monthly_zones_revenue not found"
