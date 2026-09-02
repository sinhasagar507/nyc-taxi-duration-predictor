"""Unit tests for credential decoupling (Phase 4).

The service-account key filename must NOT be hardcoded into code/config, so that
swapping GCP projects is a one-file drop at the stable path
`secrets/gcp-credentials.json` plus a single `.env` edit — not a repo-wide hunt.

These are guard tests: they scan tracked code/config and fail if a project-specific
credential filename appears. Docs are exempt — they legitimately narrate history.

**Generalised 2026-09-02.** This used to forbid one literal keyfile stem. Forbidding a
single instance protects against exactly one mistake: re-committing that one file. The
guard now forbids the *shape* instead, which is strictly stronger — it catches any
project's key, including one downloaded tomorrow, and it carries no account identifier
in the test file.

A denylist that names nothing has a failure mode a literal one does not: the pattern can
rot into something that matches nothing, and the guard passes forever while protecting
nothing. So each pattern has a **positive control** below, asserting it still catches a
synthetic example. Delete a control and you lose the proof.
"""
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

STABLE_CRED_NAME = "gcp-credentials.json"

# A GCP service-account key downloads as `<project-id>-<12 hex>.json`. That shape is what
# lands in a repository by accident, because it is the filename the console hands you.
SA_KEYFILE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*-[0-9a-f]{12}\.json")

# Any credential under secrets/ that is not the one stable name. Catches the other half:
# a key that was renamed to something friendly and then hardcoded anyway.
SECRETS_JSON_PATTERN = re.compile(r"secrets/([A-Za-z0-9_.-]+\.json)")

# Synthetic, not real. Used only to prove the patterns still bite.
SYNTHETIC_KEYFILE = "example-project-0123456789ab.json"

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
def test_no_hardcoded_service_account_keyfile(rel_path):
    """No code/config file may embed a downloaded service-account key filename."""
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        pytest.skip(f"{rel_path} not present")
    found = SA_KEYFILE_PATTERN.findall(path.read_text())
    assert not found, (
        f"{rel_path} hardcodes a service-account key filename: {found}. "
        f"Reference credentials via the stable '{STABLE_CRED_NAME}' path / "
        f"GOOGLE_APPLICATION_CREDENTIALS env var instead."
    )


@pytest.mark.parametrize("rel_path", SCANNED_FILES)
def test_only_the_stable_credential_name_appears_under_secrets(rel_path):
    """A key renamed to something friendly is still a hardcoded key."""
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        pytest.skip(f"{rel_path} not present")
    names = set(SECRETS_JSON_PATTERN.findall(path.read_text()))
    unexpected = sorted(names - {STABLE_CRED_NAME})
    assert not unexpected, (
        f"{rel_path} points at secrets/{unexpected} instead of "
        f"secrets/{STABLE_CRED_NAME}. One stable path, or swapping projects becomes a "
        "repo-wide hunt again."
    )


class TestTheGuardStillBites:
    """Positive controls. Without these, a broken pattern passes silently forever."""

    def test_keyfile_pattern_catches_a_downloaded_key(self):
        assert SA_KEYFILE_PATTERN.search(f"secrets/{SYNTHETIC_KEYFILE}")

    def test_keyfile_pattern_allows_the_stable_name(self):
        """The guard must not fire on the very filename it is steering people toward."""
        assert not SA_KEYFILE_PATTERN.search(f"secrets/{STABLE_CRED_NAME}")

    def test_secrets_pattern_catches_a_renamed_key(self):
        names = SECRETS_JSON_PATTERN.findall("path = 'secrets/my-own-key.json'")
        assert names == ["my-own-key.json"]

    def test_secrets_pattern_accepts_the_stable_name(self):
        names = SECRETS_JSON_PATTERN.findall(f"path = 'secrets/{STABLE_CRED_NAME}'")
        assert names == [STABLE_CRED_NAME]


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
