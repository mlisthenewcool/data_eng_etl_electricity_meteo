# Airflow Docker image with psycopg v3 (not psycopg2).
#
# References:
#   - https://airflow.apache.org/docs/docker-stack/build.html
#   - https://airflow.apache.org/docs/docker-stack/build-arg-ref.html
#   - https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html

FROM apache/airflow:latest-python3.13

# --------------------------------------------------------------------------------------
# System dependencies (as root)
# --------------------------------------------------------------------------------------

USER root

# Build deps for "psycopg[c]" (C-accelerated PostgreSQL adapter)
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends libpq-dev gcc python3-dev \
    && apt-get autoremove --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Pre-create data directory with correct ownership before volume mount.
# Without this, mkdir(parents=True) fails at runtime
# (airflow user can't create dirs under /opt/airflow/).
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data

# --------------------------------------------------------------------------------------
# Python dependencies (as airflow user)
# --------------------------------------------------------------------------------------

USER airflow

RUN pip install --no-cache-dir --upgrade uv && uv python upgrade

RUN uv pip install --no-cache \
    dbt-postgres duckdb "httpx[http2]" orjson polars "psycopg[c]" \
    py7zr pyarrow pydantic pydantic-settings pyyaml structlog tqdm

# Pre-install DuckDB extensions (avoids download at runtime)
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL spatial; INSTALL parquet;')"

# --------------------------------------------------------------------------------------
# Airflow configuration (structural — same in dev and prod)
# --------------------------------------------------------------------------------------

# -- Path ------------------------------------------------------------------------------
ENV PYTHONPATH="/opt/airflow/src"

# -- Core ------------------------------------------------------------------------------
# TODO[prod]: set AIRFLOW__CORE__DAGS_FOLDER instead of bind-mounting dags/
ENV AIRFLOW__CORE__EXECUTOR="LocalExecutor" \
    AIRFLOW__CORE__DEFAULT_TIMEZONE="UTC" \
    AIRFLOW__CORE__LOAD_EXAMPLES="False" \
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG="1" \
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG="16" \
    AIRFLOW__CORE__PARALLELISM="32"

# -- API -------------------------------------------------------------------------------
# TODO[prod]: set EXPOSE_CONFIG to False (exposes connection strings via REST API)
ENV AIRFLOW__API__EXPOSE_CONFIG="True"

# -- DAG processor ---------------------------------------------------------------------
ENV AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT="120" \
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="30" \
    AIRFLOW__DAG_PROCESSOR__BUNDLE_REFRESH_CHECK_INTERVAL="5"

# --------------------------------------------------------------------------------------
# Build verification (visible in `docker buildx history logs`)
# --------------------------------------------------------------------------------------

RUN uv pip list && uv pip list --outdated
