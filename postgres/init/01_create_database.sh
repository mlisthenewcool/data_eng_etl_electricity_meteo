#!/usr/bin/env bash
set -euo pipefail

# Database initialization script
# Executed only when Postgres volume is empty (first run)

# Note: Default database (POSTGRES_DB / airflow) is created automatically by the container.
# This script creates the additional project database.

export PGUSER="${POSTGRES_USER}"
export PGDATABASE="${POSTGRES_DB}"

DB_EXISTS=$(psql -v ON_ERROR_STOP=1 -tAc "SELECT 1 FROM pg_database WHERE datname='${PROJECT_DB_NAME}'")

if [ "$DB_EXISTS" != "1" ]; then
  echo "Create database '${PROJECT_DB_NAME}' ..."
  psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${PROJECT_DB_NAME}\";"
else
  echo "Database '${PROJECT_DB_NAME}' already exists. Skipping."
fi