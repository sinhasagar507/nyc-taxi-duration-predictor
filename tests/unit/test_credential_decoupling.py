"""Unit tests for credential decoupling (Phase 4).

The service-account key filename must NOT be hardcoded into code/config, so that
swapping GCP projects/accounts is a one-file drop at the stable path
`secrets/gcp-credentials.json` plus a single `.env` edit — not a repo-wide hunt.

These are guard tests: they scan tracked code/config for the old, project-ID-embedded
keyfile stem and fail if it reappears. Docs and the local `migration_backup/` are
exempt (docs legitimately narrate history; the backup is gitignored data).
"""
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

# The old key filename was `dtc-de-project-492321-970e67a252d8.json`. Forbid its stem
# (which uniquely identifies that specific key) from code/config.
OLD_KEYFILE_STEM = "dtc-de-project-492321-970e67a252d8"

STABLE_CRED_NAME = "gcp-credentials.json"

# Code/config files that must reference credentials only via the stable path / env var.
SCANNED_FILES = [
    "tests/conftest.py",
    "dbt/profiles.yml",
    "terraform/variables.tf",
    "airflow/docker-compose.yaml",
    ".github/workflows/dbt.yml",
    "spark/bigquery_connector.py",
    "spark/pyspark_bigquery_hybrid.py",
]


@pytest.mark.parametrize("rel_path", SCANNED_FILES)
def test_no_hardcoded_old_keyfile_name(rel_path):
    """No code/config file may embed the old project-specific key filename."""
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        pytest.skip(f"{rel_path} not present")
    text = path.read_text()
    assert OLD_KEYFILE_STEM not in text, (
        f"{rel_path} hardcodes the old keyfile name '{OLD_KEYFILE_STEM}'. "
        f"Reference credentials via the stable '{STABLE_CRED_NAME}' path / "
        f"GOOGLE_APPLICATION_CREDENTIALS env var instead."
    )


def test_conftest_resolves_stable_credential_path():
    """conftest's credential path must point at the stable, project-agnostic filename."""
    import tests.conftest as conftest

    assert conftest.CREDS_PATH.name == STABLE_CRED_NAME, (
        f"conftest.CREDS_PATH should resolve to '{STABLE_CRED_NAME}', "
        f"got '{conftest.CREDS_PATH.name}'"
    )


def test_project_and_bucket_read_from_env(monkeypatch):
    """conftest must let GCP_PROJECT_ID / GCP_GCS_BUCKET override via env (single source)."""
    import importlib

    monkeypatch.setenv("GCP_PROJECT_ID", "some-other-project")
    monkeypatch.setenv("GCP_GCS_BUCKET", "some-other-bucket")
    import tests.conftest as conftest

    importlib.reload(conftest)
    try:
        assert conftest.GCP_PROJECT == "some-other-project"
        assert conftest.GCS_BUCKET == "some-other-bucket"
    finally:
        monkeypatch.undo()
        importlib.reload(conftest)
