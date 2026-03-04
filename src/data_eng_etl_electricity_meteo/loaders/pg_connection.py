"""Postgres connection factories for standalone and Airflow environments.

This module is the **only** place that knows how to obtain a Postgres connection.
The actual loading logic in ``pg_loader`` receives an open ``psycopg.Connection`` and is
Airflow-agnostic.

Two factories are provided:

- ``open_standalone_connection()`` — for scripts and tests. Reads credentials from
  pydantic-settings (env vars or Docker secrets).
- ``open_airflow_hook_connection()`` — for Airflow tasks. Extracts a
  ``psycopg.Connection`` from a ``PostgresHook``.

Both return a ``psycopg.Connection`` — callers are responsible for closing it.

Why ``get_conn()`` instead of Hook convenience methods?
-------------------------------------------------------
The loader needs a single atomic transaction spanning DDL, schema validation,
TRUNCATE/staging, COPY streaming, and upsert. Hook methods (``run``, ``copy_expert``)
are stateless per call and do not share a transaction. The Hook's ``get_conn()`` is the
documented pattern for complex operations requiring fine-grained transaction control.
"""

from typing import TYPE_CHECKING, Any, cast

import psycopg

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_eng_etl_electricity_meteo.core.exceptions import (
    PostgresCredentialsError,
)
from data_eng_etl_electricity_meteo.core.settings import settings


def open_standalone_connection() -> psycopg.Connection[Any]:
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


def open_airflow_hook_connection(hook: "PostgresHook") -> psycopg.Connection[Any]:
    """Extract a ``psycopg.Connection`` from an Airflow ``PostgresHook``.

    ``PostgresHook.get_conn()`` returns a psycopg3 connection wrapped in
    ``CompatConnection`` (Airflow's psycopg2/3 abstraction layer) when ``USE_PSYCOPG3``
    is ``True`` — which is guaranteed when psycopg3 and SQLAlchemy 2.x are both
    installed (always the case in this project).

    The cast is necessary because ``CompatConnection`` is not recognized by type
    checkers as a ``psycopg.Connection``.

    Parameters
    ----------
    hook
        Airflow ``PostgresHook`` configured for the project database.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it.
    """
    return cast("psycopg.Connection[Any]", cast("object", hook.get_conn()))
