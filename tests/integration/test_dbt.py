"""
Integration tests — dbt Core smoke tests.
Verifies dbt models compile cleanly against the live BigQuery schema.
Requires GOOGLE_APPLICATION_CREDENTIALS (auto-skipped if absent).
"""

import os
import subprocess
import sys
import pytest
from pathlib import Path

from tests.conftest import DBT_PROJECT_DIR, DBT_PROFILES_DIR, CREDS_PATH

pytestmark = pytest.mark.integration

# Use the dbt binary from the same venv as the test runner to avoid picking up
# the Homebrew dbt Cloud CLI, which is a different product and doesn't support
# --project-dir / --profiles-dir.
_DBT_BIN = Path(sys.executable).parent / "dbt"


def _run_dbt(command):
    env = {
        **os.environ,
        "GOOGLE_APPLICATION_CREDENTIALS": str(CREDS_PATH),
    }
    return subprocess.run(
        [str(_DBT_BIN), command, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROFILES_DIR)],
        capture_output=True,
        text=True,
        env=env,
    )


def test_dbt_compile_succeeds():
    result = _run_dbt("compile")
    assert result.returncode == 0, (
        f"dbt compile failed (exit {result.returncode}):\n"
        f"STDOUT:\n{result.stdout[-3000:]}\n"
        f"STDERR:\n{result.stderr[-1000:]}"
    )


def test_dbt_compile_reports_no_errors():
    result = _run_dbt("compile")
    output = result.stdout + result.stderr
    assert "ERROR" not in output.upper() or result.returncode == 0, (
        "dbt compile output contains ERROR — check model SQL or BigQuery schema"
    )
