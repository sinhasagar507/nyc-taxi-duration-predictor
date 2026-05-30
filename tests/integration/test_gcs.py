"""
Integration tests — GCS bucket smoke tests.
Verifies the live bucket is reachable and all expected data files are present.
Requires GOOGLE_APPLICATION_CREDENTIALS (auto-skipped if absent).
"""

import pytest
from google.cloud import storage

from tests.conftest import GCS_BUCKET

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def gcs(request):
    return storage.Client()


class TestBucketReachable:
    def test_primary_data_bucket_exists(self, gcs):
        assert gcs.bucket(GCS_BUCKET).exists(), f"Bucket '{GCS_BUCKET}' not found"


class TestYellowTaxiData:
    def test_yellow_taxi_has_24_parquet_files(self, gcs):
        blobs = list(gcs.list_blobs(GCS_BUCKET, prefix="nyc_taxi_data/yellow_taxi_data/"))
        parquet = [b for b in blobs if b.name.endswith(".parquet")]
        assert len(parquet) == 24, (
            f"Expected 24 yellow taxi parquet files (2015-01 to 2016-12), found {len(parquet)}"
        )


class TestGreenTaxiData:
    def test_green_taxi_has_24_parquet_files(self, gcs):
        blobs = list(gcs.list_blobs(GCS_BUCKET, prefix="nyc_taxi_data/green_taxi_data/"))
        parquet = [b for b in blobs if b.name.endswith(".parquet")]
        assert len(parquet) == 24, (
            f"Expected 24 green taxi parquet files (2015-01 to 2016-12), found {len(parquet)}"
        )


class TestReferenceData:
    def test_taxi_zone_csv_exists(self, gcs):
        blobs = list(gcs.list_blobs(GCS_BUCKET, prefix="nyc_taxi_data/taxi_lookup_data/taxi_zone_lookup.csv"))
        assert len(blobs) == 1, "taxi_zone_lookup.csv not found in GCS"

    def test_climate_parquet_exists(self, gcs):
        blobs = list(gcs.list_blobs(GCS_BUCKET, prefix="nyc_climate_data/climate_data.parquet"))
        assert len(blobs) == 1, "climate_data.parquet not found in GCS"
