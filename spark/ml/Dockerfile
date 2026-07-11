# NYC-taxi fare-prediction ML environment.
# Single container for the whole modeling workflow: PySpark (batch prep) + the
# pandas/scikit-learn stack + JupyterLab (interactive model sweep). Per the
# Docker-first policy, all ML libraries live here, not on the host.
#
# Python 3.12 (Spark's officially supported line) + JRE 17 (required by Spark 4.x).
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/workspace

# default-jre-headless -> Java 17 (Spark 4.x); procps -> `ps`, which Spark shells out to
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Repo is bind-mounted at /workspace by docker-compose; nothing is copied in.
CMD ["bash"]
