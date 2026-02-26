#!/usr/bin/env bash
set -euo pipefail

# Database initialization script.
# Executed only when Postgres volume is empty (first run).
# Note: POSTGRES_DB (airflow) is created automatically by the container entrypoint.
# This script creates the additional project database.

export PGUSER="${POSTGRES_USER}"
export PGDATABASE="${POSTGRES_DB}"  # connect to airflow DB to inspect pg_database catalog

DB_EXISTS=$(psql -v ON_ERROR_STOP=1 -tAc "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB_NAME}'")

if [ "${DB_EXISTS}" != "1" ]; then
    echo "Creating database '${POSTGRES_DB_NAME}' ..."
    psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${POSTGRES_DB_NAME}\";"
    echo "Database '${POSTGRES_DB_NAME}' created."
else
    echo "Database '${POSTGRES_DB_NAME}' already exists. Skipping."
fi
