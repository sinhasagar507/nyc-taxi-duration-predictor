"""Unit tests for spark/ml/src/paths.py — where the fact_trips backup lives.

Audit item 10 moved `migration_backup/` (7.1 GB) out of the working tree. The
prep script used to hard-code `REPO_ROOT / "migration_backup"`, so the move
would have broken it silently: Spark reports a missing input directory the same
way whether the path is wrong or the data is gone.

The resolution rule is one line of policy and worth a test of its own:

  1. `MIGRATION_BACKUP_DIR`, if set, wins outright — that is how a different
     machine, an external disk, or a container mount points the prep somewhere
     else without editing code.
  2. Otherwise the default is the repo's **sibling** directory
     `nyc_taxi_migration_backup/`, which is where item 10 put it.

Both branches are tested because the fallback is the one that runs unattended
and the override is the one that runs on someone else's machine.
"""

from pathlib import Path

from spark.ml.src import paths


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_default_backup_dir_is_the_repo_sibling():
    """With no env var set, the backup sits beside the repo, not inside it."""
    resolved = paths.resolve_backup_dir(env={})

    assert resolved == REPO_ROOT.parent / "nyc_taxi_migration_backup"


def test_default_backup_dir_is_outside_the_working_tree():
    """The whole point of item 10: the default must not be under the repo."""
    resolved = paths.resolve_backup_dir(env={})

    assert not resolved.is_relative_to(REPO_ROOT)


def test_env_var_overrides_the_default(tmp_path):
    """MIGRATION_BACKUP_DIR wins, so a mount or an external disk needs no edit."""
    resolved = paths.resolve_backup_dir(env={"MIGRATION_BACKUP_DIR": str(tmp_path)})

    assert resolved == tmp_path


def test_env_var_is_expanded_and_absolute(monkeypatch, tmp_path):
    """`~` and a relative path both resolve — a half-resolved path fails late."""
    monkeypatch.setenv("HOME", str(tmp_path))

    resolved = paths.resolve_backup_dir(env={"MIGRATION_BACKUP_DIR": "~/backup"})

    assert resolved == tmp_path / "backup"
    assert resolved.is_absolute()


def test_blank_env_var_falls_back_to_the_default():
    """An exported-but-empty var is not a path. Treat it as unset."""
    resolved = paths.resolve_backup_dir(env={"MIGRATION_BACKUP_DIR": "   "})

    assert resolved == REPO_ROOT.parent / "nyc_taxi_migration_backup"


def test_fact_trips_dir_hangs_off_the_resolved_backup(tmp_path):
    """The prep reads one subdirectory of the backup; keep the join in one place."""
    resolved = paths.resolve_fact_trips_dir(env={"MIGRATION_BACKUP_DIR": str(tmp_path)})

    assert resolved == tmp_path / "fact_trips"


def test_repo_root_points_at_the_repository():
    """paths.REPO_ROOT anchors every other path; a wrong anchor breaks all of them."""
    assert (paths.REPO_ROOT / "CLAUDE.md").exists()
    assert paths.REPO_ROOT == REPO_ROOT
