"""Guard tests for M0 of the GCP cloud migration plan (`notes/2026-09-02-...`).

Two pieces of configuration hold identifiers that point at projects this repository does
not use. Both are invisible until something tries to run against them, which is the worst
time to find out. These guards make them visible now, and keep them gone afterwards.

**1. Terraform state must not be tracked by git.** Three `*.tfstate*` files are committed.
That is wrong twice over:

  - State files can carry resource attributes that are not meant to be shared, and they are
    machine-written, so nobody reviews their diffs.
  - `CLAUDE.md` says the state is empty, and it is not. The tracked files describe real
    resources under foreign projects. An engineer who trusts that sentence and runs
    `terraform apply` gets a plan built from someone else's inventory.

**2. The dbt submodule's staging schemas must not hardcode a project.** Both source
definitions name a literal project as their `database:`. The env var `GCP_PROJECT_ID` is
the single source of truth everywhere else in this repository, and dbt is the one place
that ignores it.

Reading inside `dbt/ny_taxi_analytics` is fine — `CLAUDE.md` forbids *editing* there, not
looking. The fix goes upstream, then the submodule pointer moves. This guard is what tells
you the pointer bump actually landed.

Per **D-009**, these assert what was measured on 2026-09-02, not what was remembered.
"""

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

TFSTATE_PATTERN = "*.tfstate*"

# The staging schemas whose `database:` key must be an env var, not a literal.
SUBMODULE_STAGING_DIR = PROJECT_ROOT / "dbt" / "ny_taxi_analytics" / "models" / "staging"

# A literal GCP project from this account's naming family, sitting in a `database:` line.
# Matching the key as well as the value keeps the guard off prose and comments that
# legitimately mention a project.
DATABASE_LITERAL = re.compile(r"^\s*database:\s*[\"']?(dtc-de-[a-z0-9][a-z0-9_-]*)", re.M)

# Synthetic, not real. Proves the pattern still bites — see D-009.
SYNTHETIC_DATABASE_LINE = "    database: dtc-de-example-000000 # comment"


def _tracked_tfstate_files() -> list[str]:
    """Every `*.tfstate*` path git currently has in its index."""
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "--", f"terraform/{TFSTATE_PATTERN}"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in out.stdout.splitlines() if line.strip()]


class TestTerraformStateIsNotTracked:
    def test_no_tfstate_file_is_tracked_by_git(self):
        """State is machine-written and environment-specific. It does not belong in git."""
        tracked = _tracked_tfstate_files()
        assert not tracked, (
            f"Terraform state is tracked by git: {tracked}. Remove it from the index "
            "(`git rm --cached`) and gitignore `terraform/*.tfstate*`. State describes one "
            "machine's view of live infrastructure; sharing it through git makes every "
            "clone disagree about what exists."
        )

    def test_gitignore_covers_terraform_state(self):
        """Untracking without ignoring only defers the problem to the next `git add -A`."""
        gitignore = (PROJECT_ROOT / ".gitignore").read_text()
        assert "terraform/*.tfstate*" in gitignore, (
            ".gitignore does not cover `terraform/*.tfstate*`. Without it, the next "
            "`git add -A` re-adds the state files that were just removed."
        )


class TestSubmoduleSchemasUseTheEnvVar:
    """Blocked on an upstream change, so these are `xfail(strict=True)`.

    The fix belongs in github.com/sinhasagar507/ny_taxi_analytics, and only the owner
    pushes there. `strict=True` is the point: the moment the submodule pointer bumps to a
    commit carrying the fix, these turn from xpass into a hard failure, and that failure is
    the instruction to delete this marker. A plain skip would go quiet forever and the
    defect would survive its own fix.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known defect, blocked upstream: both staging schemas hardcode a project as "
            "`database:`. Fix in ny_taxi_analytics, bump the submodule pointer, then "
            "remove this marker. Migration plan M0."
        ),
    )
    @pytest.mark.parametrize("schema", ["schema_taxi.yml", "schema_climate.yml"])
    def test_staging_schema_does_not_hardcode_a_project(self, schema):
        """`database:` must resolve from GCP_PROJECT_ID like everything else does."""
        path = SUBMODULE_STAGING_DIR / schema
        if not path.exists():
            pytest.skip(f"{schema} not present — submodule not checked out")
        found = sorted(set(DATABASE_LITERAL.findall(path.read_text())))
        assert not found, (
            f"{schema} hardcodes a project as its source database: {found}. Fix it "
            "upstream in ny_taxi_analytics with "
            "`database: \"{{ env_var('GCP_PROJECT_ID') }}\"`, then bump the submodule "
            "pointer here. Do not edit inside the submodule directory."
        )


class TestTheGuardStillBites:
    """Positive controls, per D-009. A pattern that matches nothing passes forever."""

    def test_database_pattern_catches_a_hardcoded_project(self):
        assert DATABASE_LITERAL.findall(SYNTHETIC_DATABASE_LINE) == [
            "dtc-de-example-000000"
        ]

    def test_database_pattern_ignores_the_env_var_form(self):
        """The fix itself must not trip the guard that asked for it."""
        fixed = "    database: \"{{ env_var('GCP_PROJECT_ID') }}\""
        assert not DATABASE_LITERAL.findall(fixed)

    def test_database_pattern_ignores_a_project_named_in_prose(self):
        """A comment or description mentioning a project is not a configuration defect."""
        prose = "# migrated away from dtc-de-example-000000 in 2026"
        assert not DATABASE_LITERAL.findall(prose)
