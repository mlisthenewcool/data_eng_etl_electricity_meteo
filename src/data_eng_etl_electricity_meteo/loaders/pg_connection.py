"""Postgres connection factories for standalone and Airflow environments.

This module is the **only** place that knows how to obtain a Postgres connection.
The actual loading logic in ``pg_loader`` receives an open ``psycopg.Connection`` and is
Airflow-agnostic.

Two factories are provided:

- ``open_standalone_connection()`` — for scripts and tests.
  Reads credentials from pydantic-settings (Docker secrets files).
- ``open_airflow_connection()`` — for Airflow tasks.
  Resolves credentials from the Airflow connection store, then calls
  ``psycopg.connect()`` directly.

Both return a native ``psycopg.Connection`` — callers are responsible for closing it.

``psycopg.connect()`` is called directly (instead of ``PostgresHook.get_conn()``)
because the loader uses psycopg3-specific APIs (binary ``COPY`` protocol) and
``get_conn()`` returns ``CompatConnection`` — a Protocol abstracting psycopg2/3 that
hides the concrete ``psycopg.Connection`` type from type checkers.  Credentials are
resolved via ``Connection.get()`` from the Airflow Task SDK — no hook needed.
"""

from __future__ import annotations

import psycopg

from data_eng_etl_electricity_meteo.core.exceptions import (
    PostgresCredentialsError,
)
from data_eng_etl_electricity_meteo.core.settings import settings

# Airflow connection id for the project Postgres database.
# Must match the env var AIRFLOW_CONN_{ID.upper()} in docker-compose.yaml.
AIRFLOW_CONN_ID = "project_postgres"


def open_standalone_connection() -> psycopg.Connection:
    """Open a psycopg connection using settings resolved at startup.

    Credentials (``settings.postgres_user`` / ``settings.postgres_password``) are
    populated by pydantic-settings from Docker secrets files in the ``secrets/``
    directory (local dev) or ``/run/secrets/`` (Docker container).
    The loader code is identical in both environments.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it.

    Raises
    ------
    PostgresCredentialsError
        If credentials are missing from Docker secrets files.
    psycopg.OperationalError
        If the connection cannot be established.
    """
    if settings.postgres_user is None:
        raise PostgresCredentialsError(
            missing_field="postgres_user",
            suggestion="Create secrets/postgres_root_username file.",
        )
    if settings.postgres_password is None:
        raise PostgresCredentialsError(
            missing_field="postgres_password",
            suggestion="Create secrets/postgres_root_password file.",
        )

    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db_name,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def open_airflow_connection() -> psycopg.Connection:
    """Resolve Airflow credentials and open a native ``psycopg.Connection``.

    Uses ``Connection.get()`` from the Airflow Task SDK to retrieve host, port, dbname,
    user and password from the Airflow connection store, then calls
    ``psycopg.connect()`` directly — the same pattern as
    ``open_standalone_connection()`` but sourcing credentials from Airflow instead of
    pydantic-settings.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it.

    Raises
    ------
    airflow.exceptions.AirflowNotFoundException
        If ``AIRFLOW_CONN_ID`` is not defined in the Airflow connection store.
    psycopg.OperationalError
        If the connection cannot be established.
    """
    # Lazy import: avoid pulling the Airflow import chain when only
    # open_standalone_connection() is used (CLI, tests).
    from airflow.sdk.definitions.connection import Connection  # noqa: PLC0415

    airflow_conn = Connection.get(AIRFLOW_CONN_ID)
    return psycopg.connect(
        host=airflow_conn.host,
        port=airflow_conn.port,
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
    )
