#!/usr/bin/env bash
set -euo pipefail

# Schema initialization for the project database.
# Executed only when Postgres volume is empty (first run), after 01_create_database.sh.

export PGUSER="${POSTGRES_USER}"
export PGDATABASE="${PROJECT_DB_NAME}"

echo "Creating schemas in '${PROJECT_DB_NAME}' ..."
psql -v ON_ERROR_STOP=1 -c "
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
"
echo "Schemas created."
