"""Postgres connection factories for standalone and Airflow environments.

This module is the **only** place that knows how to obtain a Postgres connection.
The actual loading logic in ``pg_loader`` receives an open ``psycopg.Connection`` and is
Airflow-agnostic.

Two factories are provided:

- ``open_standalone_connection()`` — for scripts and tests.
  Reads credentials from pydantic-settings (env vars or Docker secrets).
- ``open_airflow_connection()`` — for Airflow tasks.
  Creates a ``PostgresHook`` from ``AIRFLOW_CONN_ID`` and extracts a
  ``psycopg.Connection``.

Both return a ``psycopg.Connection`` — callers are responsible for closing it.

``get_conn()`` is used instead of Hook convenience methods (``run``, ``copy_expert``)
because the loader needs a single atomic transaction spanning DDL, schema validation,
TRUNCATE/staging, COPY streaming, and upsert.
Hook methods are stateless per call and do not share a transaction.
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
    populated by pydantic-settings from whichever source was available: env vars
    (local dev) or Docker secrets files (Docker / Airflow container).
    The loader code is identical in both environments.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it.

    Raises
    ------
    PostgresCredentialsError
        If credentials are missing from both env vars and Docker secrets.
    psycopg.OperationalError
        If the connection cannot be established.
    """
    if settings.postgres_user is None:
        raise PostgresCredentialsError(
            missing_field="postgres_user",
            suggestion=(
                "Set POSTGRES_USER env var or provide a 'postgres_root_username' Docker secret."
            ),
        )
    if settings.postgres_password is None:
        raise PostgresCredentialsError(
            missing_field="postgres_password",
            suggestion=(
                "Set POSTGRES_PASSWORD env var or provide a 'postgres_root_password' Docker secret."
            ),
        )

    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db_name,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def open_airflow_connection() -> psycopg.Connection:
    """Create a ``PostgresHook`` and return a ``psycopg.Connection``.

    Uses ``AIRFLOW_CONN_ID`` to look up the Airflow connection store.
    ``PostgresHook.get_conn()`` returns a psycopg3 connection wrapped in
    ``CompatConnection`` (Airflow's psycopg2/3 abstraction layer) when ``USE_PSYCOPG3``
    is ``True`` — which is guaranteed when psycopg3 and SQLAlchemy 2.x are both
    installed (always the case in this project).

    The ``type: ignore[assignment]`` is necessary because ``CompatConnection`` is not
    recognized by type checkers as a ``psycopg.Connection``.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it.
    """
    # Lazy import: Airflow providers only available inside the container.
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: PLC0415

    hook = PostgresHook(AIRFLOW_CONN_ID)
    conn: psycopg.Connection = hook.get_conn()  # type: ignore[assignment]
    return conn
