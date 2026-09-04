# Whole-repo development runtime. Dev-only — this image is never deployed.
#
# One container for: the ML stack (pandas/scikit-learn/XGBoost/LightGBM/CatBoost/
# SHAP/Optuna, later torch), PySpark batch prep, the pytest suite, the dbt CLI and
# JupyterLab. Airflow has its own separate image (airflow/dockerfile) — the two
# stacks are never merged and never share an image.
#
# Python 3.12 (Spark's supported line) + JRE 17 (required by Spark 4.x).
# The build context is the REPO ROOT because the three requirements files live in
# three different subtrees; the root .dockerignore keeps that context tiny.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/workspace

# default-jre-headless -> Java 17 (Spark 4.x)
# procps               -> `ps`, which Spark shells out to
# libgomp1             -> OpenMP runtime; the xgboost/lightgbm/catboost wheels link
#                         against libgomp.so.1 and fail at IMPORT time without it
# git                  -> dbt deps / submodule inspection from inside the container
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      default-jre-headless procps libgomp1 git \
 && rm -rf /var/lib/apt/lists/*

# --- Layer 1: dev harness (changes rarely) -----------------------------------
COPY docker/dev/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt

# --- Layer 2: dbt CLI in an ISOLATED venv ------------------------------------
# Mirrors airflow/dockerfile: dbt's protobuf / google-cloud pins never touch the
# site-packages where pyspark and scikit-learn live. Installed FROM
# dbt/requirements.txt so the version is single-sourced, not re-typed here.
# The symlink matters: tests/integration/test_dbt.py resolves the dbt binary as
# Path(sys.executable).parent / "dbt", which is /usr/local/bin/dbt in this image.
COPY dbt/requirements.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

# --- Layer 3: ML / Spark stack (churns as modeling phases add libraries) -----
COPY spark/ml/requirements.txt /tmp/requirements-ml.txt
RUN pip install --no-cache-dir -r /tmp/requirements-ml.txt

WORKDIR /workspace

# The repo is bind-mounted at /workspace by compose; no source is copied in.
CMD ["bash"]
