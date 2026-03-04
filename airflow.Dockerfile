# Note : This build use psycopg (v3) instead of psycopg2.

# https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html
# https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
# https://airflow.apache.org/docs/docker-stack/build.html

FROM apache/airflow:latest-python3.13

USER root

# --- UPDATE/UPGRADE ---
# install dependencies to build "psycopg[c]" module (best performance)
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends libpq-dev gcc python3-dev \
    && apt-get autoremove --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create the data directory and assign ownership to the 'airflow' user
# before volume mounting. This ensures the application has the necessary
# permissions to create subdirectories at runtime, preventing failures in
# dest_path.parent.mkdir(parents=True, exist_ok=True)
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data
# COPY data/catalog.yaml /opt/airflow/data/catalog.yaml

USER airflow
RUN pip install --no-cache-dir --upgrade uv
RUN uv python upgrade

# --- CONFIGURATION OPTIONS ---
# Config structurelle qui ne change pas entre dev et prod (executor, timezone, comportement des DAGs)
# https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html

# - AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG : prevent file conflicts within same dataset

# TODO: configurer en production -> AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/src/de_projet_perso/airflow/dags
ENV PYTHONPATH="/opt/airflow/src" \
    AIRFLOW__CORE__EXECUTOR="LocalExecutor" \
    AIRFLOW__CORE__DEFAULT_TIMEZONE="Europe/Paris" \
    AIRFLOW__CORE__LOAD_EXAMPLES="False" \
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG="1" \
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG="16" \
    AIRFLOW__CORE__PARALLELISM="32" \
    AIRFLOW__API__EXPOSE_CONFIG="True" \
    AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT="120" \
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="30" \
    AIRFLOW__DAG_PROCESSOR__BUNDLE_REFRESH_CHECK_INTERVAL="5"

RUN uv pip install --no-cache \
    duckdb "httpx[http2]" orjson polars "psycopg[c]" py7zr pyarrow \
    pydantic pydantic-settings pyaml structlog tqdm \
    "dbt-postgres>=1.10,<2"

# Install DuckDB extensions once
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL spatial; INSTALL parquet;')"

RUN uv pip list
RUN uv pip list --outdated
