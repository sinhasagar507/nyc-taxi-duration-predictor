"""Unit tests for the whole-repo dev container runtime (Phase: dev-container).

The dev image at `docker/dev/` is where every Python library lives — the ML stack,
the pytest harness, and the dbt CLI. These are guard tests: they read the runtime's
files and fail if an invariant that is expensive to rediscover is broken. None of
them need a Docker daemon, so they run on the host, in CI, and inside the container.

The invariants worth locking, and why each one earns its place:

* The build context is the repo root, which holds a gitignored 7.1 GB
  `migration_backup/`. A `.dockerignore` regression would silently ship that to the
  daemon on every build.
* `dbt-bigquery` is pinned in `dbt/requirements.txt` and consumed by two images.
  The Airflow dockerfile asks for sync in a comment; nothing enforced it until now.
* `libgomp1` is an apt package whose absence produces a *green build* that
  ImportErrors on `import xgboost` — invisible until runtime.
* JupyterLab is served out of a root container with the whole repo (including
  `secrets/`) mounted. Binding it to anything but loopback exposes that to the LAN.
"""
import re
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.parent

DOCKERFILE = PROJECT_ROOT / "docker" / "dev" / "Dockerfile"
COMPOSE = PROJECT_ROOT / "docker" / "dev" / "docker-compose.yml"
DEV_REQS = PROJECT_ROOT / "docker" / "dev" / "requirements-dev.txt"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
ML_REQS = PROJECT_ROOT / "spark" / "ml" / "requirements.txt"
DBT_REQS = PROJECT_ROOT / "dbt" / "requirements.txt"
AIRFLOW_DOCKERFILE = PROJECT_ROOT / "airflow" / "dockerfile"
AIRFLOW_COMPOSE = PROJECT_ROOT / "airflow" / "docker-compose.yaml"
CLAUDE_MD = PROJECT_ROOT / "CLAUDE.md"
README = PROJECT_ROOT / "README.md"

BASE_IMAGE = "python:3.12-slim-bookworm"

# Installed in the dev image only — never in the host .venv.
ML_LIBS = ["xgboost", "lightgbm", "catboost", "shap", "optuna"]

# The only files the build context needs; everything else arrives via bind mount.
DOCKERIGNORE_EXCEPTIONS = {
    "docker/dev/requirements-dev.txt",
    "dbt/requirements.txt",
    "spark/ml/requirements.txt",
}

# Paths that must never be re-admitted to the build context by a `!` exception.
HEAVY_PATHS = re.compile(
    r"migration_backup|\.venv|logs|/data|google-cloud-sdk|secrets|dbt_packages"
)

# Superseded by docker/dev/ — the ML-scoped runtime was never built.
RETIRED_FILES = [
    "spark/ml/Dockerfile",
    "spark/ml/docker-compose.yml",
    "spark/ml/.dockerignore",
]


def _requirement_lines(path):
    """Non-comment, non-blank lines of a requirements file."""
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _dockerignore_lines():
    return [
        line.strip()
        for line in DOCKERIGNORE.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


@pytest.fixture(scope="module")
def compose():
    """Parsed docker/dev/docker-compose.yml."""
    return yaml.safe_load(COMPOSE.read_text())


@pytest.fixture(scope="module")
def dev_service(compose):
    assert "dev" in compose["services"], "compose must define a service named 'dev'"
    return compose["services"]["dev"]


# --- the runtime exists and is pinned ----------------------------------------

@pytest.mark.parametrize(
    "path", [DOCKERFILE, COMPOSE, DEV_REQS, DOCKERIGNORE], ids=lambda p: p.name
)
def test_dev_runtime_files_exist(path):
    """Every file the dev runtime is made of must be present."""
    assert path.exists(), f"{path.relative_to(PROJECT_ROOT)} is missing"


def test_base_image_is_pinned():
    """Base image must be an exact tag, mirroring the Airflow image's patch pinning."""
    match = re.search(r"^FROM\s+(\S+)", DOCKERFILE.read_text(), re.MULTILINE)
    assert match, "Dockerfile has no FROM line"
    assert match.group(1) == BASE_IMAGE, (
        f"base image should be '{BASE_IMAGE}', got '{match.group(1)}'"
    )


def test_openmp_runtime_installed():
    """xgboost/lightgbm/catboost wheels link against libgomp.so.1.

    python:3.12-slim does not ship it, so omitting this package yields an image that
    builds cleanly and then fails at `import xgboost`.
    """
    assert "libgomp1" in DOCKERFILE.read_text(), (
        "Dockerfile must apt-install libgomp1 or the boosting libraries fail at import"
    )


# --- dbt: single-sourced pin, isolated env, on PATH ---------------------------

def test_dbt_pin_is_single_sourced():
    """The dev image installs dbt FROM dbt/requirements.txt, never an inline pin."""
    text = DOCKERFILE.read_text()
    assert "dbt/requirements.txt" in text, (
        "dev Dockerfile must COPY dbt/requirements.txt so the pin has one source"
    )
    assert not re.search(r"dbt-bigquery==", text), (
        "dev Dockerfile must not re-type the dbt-bigquery pin; install from "
        "dbt/requirements.txt instead"
    )


def test_airflow_dbt_pin_matches_requirements():
    """Airflow's inline dbt pin must agree with dbt/requirements.txt.

    The Airflow dockerfile asks for this in a comment; this test enforces it.
    """
    declared = re.search(r"dbt-bigquery==(\S+)", DBT_REQS.read_text())
    assert declared, "dbt/requirements.txt must pin dbt-bigquery"
    inline = re.search(r"dbt-bigquery==(\S+)", AIRFLOW_DOCKERFILE.read_text())
    assert inline, "airflow/dockerfile must pin dbt-bigquery"
    assert inline.group(1) == declared.group(1), (
        f"airflow/dockerfile pins dbt-bigquery=={inline.group(1)} but "
        f"dbt/requirements.txt says {declared.group(1)}"
    )


def test_dbt_lives_in_an_isolated_venv():
    """dbt's protobuf/google-cloud pins must not share site-packages with pyspark."""
    assert "/opt/dbt-venv" in DOCKERFILE.read_text(), (
        "dbt must be installed into an isolated /opt/dbt-venv, mirroring airflow/dockerfile"
    )
    for reqs in (ML_REQS, DEV_REQS):
        assert not any("dbt" in line for line in _requirement_lines(reqs)), (
            f"{reqs.relative_to(PROJECT_ROOT)} must not install dbt into the main env"
        )


def test_dbt_is_symlinked_onto_path():
    """tests/integration/test_dbt.py resolves the binary as sys.executable's sibling.

    In the container that is /usr/local/bin/dbt, so the venv binary must be linked
    there or that integration test fails inside the container.
    """
    assert re.search(
        r"ln\s+-s\s+/opt/dbt-venv/bin/dbt\s+/usr/local/bin/dbt", DOCKERFILE.read_text()
    ), "Dockerfile must symlink /opt/dbt-venv/bin/dbt to /usr/local/bin/dbt"


# --- build context: the 7.1 GB guard -----------------------------------------

def test_dockerignore_denies_everything_by_default():
    """Deny-all first line, so a future large directory is excluded automatically."""
    lines = _dockerignore_lines()
    assert lines and lines[0] == "*", (
        ".dockerignore must start with '*' (deny-all) and re-admit only what the "
        "build needs; got first effective line: "
        f"{lines[0] if lines else '<empty>'}"
    )


def test_dockerignore_readmits_only_requirements_files():
    """The allowlist must be exactly the three requirements files, all of which exist."""
    exceptions = {line[1:] for line in _dockerignore_lines() if line.startswith("!")}
    assert exceptions == DOCKERIGNORE_EXCEPTIONS, (
        f"expected exactly {sorted(DOCKERIGNORE_EXCEPTIONS)}, got {sorted(exceptions)}"
    )
    for rel in exceptions:
        assert (PROJECT_ROOT / rel).exists(), (
            f".dockerignore re-admits '{rel}', which does not exist — stale exception"
        )


def test_dockerignore_never_readmits_heavy_paths():
    """No exception may pull migration_backup/ (7.1 GB), .venv/, or secrets/ back in."""
    for line in _dockerignore_lines():
        if line.startswith("!"):
            assert not HEAVY_PATHS.search(line), (
                f".dockerignore re-admits a heavy or sensitive path: '{line}'"
            )


# --- compose shape ------------------------------------------------------------

def test_compose_build_context_is_repo_root(dev_service):
    """Context must be the repo root; the three requirements files live in three subtrees."""
    context = (COMPOSE.parent / dev_service["build"]["context"]).resolve()
    assert context == PROJECT_ROOT.resolve(), (
        f"build context resolves to {context}, expected the repo root {PROJECT_ROOT}"
    )
    dockerfile = PROJECT_ROOT / dev_service["build"]["dockerfile"]
    assert dockerfile.exists(), f"build.dockerfile points at missing {dockerfile}"


def test_compose_mounts_repo_at_workspace(dev_service):
    """The repo is bind-mounted so host edits are live in the container."""
    mounts = [v for v in dev_service["volumes"] if v.endswith(":/workspace")]
    assert len(mounts) == 1, "expected exactly one repo-root -> /workspace bind mount"
    host_side = (COMPOSE.parent / mounts[0].split(":")[0]).resolve()
    assert host_side == PROJECT_ROOT.resolve(), (
        f"mount host side resolves to {host_side}, expected {PROJECT_ROOT}"
    )


def test_compose_sets_pythonpath(dev_service):
    """PYTHONPATH=/workspace keeps `from spark.ml.src import ...` resolvable."""
    assert dev_service["environment"]["PYTHONPATH"] == "/workspace"


# --- Jupyter exposure ---------------------------------------------------------

def test_jupyter_is_published_on_loopback_only(dev_service):
    """A root container serving the whole repo must not be reachable from the LAN."""
    assert dev_service["ports"] == ["127.0.0.1:8888:8888"], (
        "Jupyter must publish to 127.0.0.1 only; got "
        f"{dev_service['ports']}"
    )


def test_jupyter_has_no_empty_token(dev_service):
    """Token auth must be on — the empty-token form is forbidden."""
    command = dev_service["command"]
    for forbidden in ("token=''", 'token=""', "--ServerApp.token="):
        assert forbidden not in command, (
            f"Jupyter command must not disable token auth (found '{forbidden}')"
        )
    assert "JUPYTER_TOKEN" in dev_service["environment"], (
        "set JUPYTER_TOKEN in the compose environment so the server requires a token"
    )


# --- requirements hygiene -----------------------------------------------------

@pytest.mark.parametrize("reqs", [ML_REQS, DEV_REQS], ids=lambda p: p.name)
def test_requirements_are_pinned(reqs):
    """Every dependency is pinned exactly or bounded — no bare package names."""
    for line in _requirement_lines(reqs):
        assert "==" in line or ("<" in line and ">=" in line), (
            f"{reqs.relative_to(PROJECT_ROOT)}: '{line}' is unpinned"
        )


@pytest.mark.parametrize("lib", ML_LIBS)
def test_ml_libraries_are_declared_once(lib):
    """The ML libraries belong to spark/ml/requirements.txt and nowhere else."""
    assert any(line.startswith(lib) for line in _requirement_lines(ML_REQS)), (
        f"{lib} must be pinned in spark/ml/requirements.txt"
    )
    assert not any(line.startswith(lib) for line in _requirement_lines(DEV_REQS)), (
        f"{lib} is a library, not harness — keep it out of requirements-dev.txt"
    )


def test_integration_test_deps_present_in_dev_requirements():
    """tests/integration/* import google.cloud at module scope.

    conftest's credential skip runs after import, so a missing client library is a
    collection ERROR inside the container rather than a clean skip.
    """
    declared = _requirement_lines(DEV_REQS)
    for pkg in ("google-cloud-bigquery", "google-cloud-storage"):
        assert any(line.startswith(pkg) for line in declared), (
            f"{pkg} must be in requirements-dev.txt or `pytest tests/` errors at "
            "collection inside the container"
        )


# --- one runtime, and Airflow left alone --------------------------------------

@pytest.mark.parametrize("rel_path", RETIRED_FILES)
def test_superseded_ml_runtime_is_gone(rel_path):
    """docker/dev/ is the single dev runtime; the ML-scoped one must not return."""
    assert not (PROJECT_ROOT / rel_path).exists(), (
        f"{rel_path} was superseded by docker/dev/ — two runtimes drift apart"
    )


@pytest.mark.parametrize(
    "rel_path", ["airflow/dockerfile", "airflow/docker-compose.yaml"]
)
def test_airflow_stack_is_separate(rel_path):
    """The Airflow stack must never reference the dev image or its compose file."""
    text = (PROJECT_ROOT / rel_path).read_text()
    for marker in ("nyc-taxi-dev", "docker/dev"):
        assert marker not in text, (
            f"{rel_path} references '{marker}'; the two Docker stacks stay separate"
        )


# --- documentation reflects the runtime ---------------------------------------

def test_docs_drop_the_airflow_only_policy():
    """CLAUDE.md's old 'Docker = Airflow only' rule is superseded by this runtime."""
    text = CLAUDE_MD.read_text()
    for stale in ("Docker = Airflow only", "do not containerize"):
        assert stale not in text, (
            f"CLAUDE.md still states the superseded policy: '{stale}'"
        )


@pytest.mark.parametrize("doc", [CLAUDE_MD, README], ids=lambda p: p.name)
def test_docs_document_the_dev_runtime(doc):
    """Both docs must show how to invoke the container."""
    assert "docker/dev/docker-compose.yml" in doc.read_text(), (
        f"{doc.name} must document the dev-container compose path"
    )


# --- runtime-only: proves isolation actually holds ----------------------------

@pytest.mark.skipif(
    not Path("/opt/dbt-venv").exists(), reason="runs inside the dev container only"
)
def test_dbt_isolation_holds_in_container():
    """Inside the image: dbt is on PATH but absent from the main interpreter.

    Checks the installed *distributions*, not `find_spec("dbt")` — the repo's own
    `dbt/` directory sits at /workspace and resolves as a PEP-420 namespace package,
    so the name is importable whether or not dbt is installed.
    """
    import importlib.metadata as md
    import shutil

    assert shutil.which("dbt"), "dbt must be on PATH via the /usr/local/bin symlink"
    for dist in ("dbt-core", "dbt-bigquery"):
        with pytest.raises(md.PackageNotFoundError):
            md.version(dist)
