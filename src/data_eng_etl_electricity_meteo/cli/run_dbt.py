"""Local dbt wrapper — exports Postgres settings as env vars and runs dbt.

dbt ``profiles.yml`` reads connection parameters via ``env_var()``, which requires real
environment variables.
This wrapper bridges pydantic-settings (which already resolves ``.env`` + secrets files)
to dbt's ``env_var()`` by exporting the resolved values before calling ``dbt``.

Usage::

    uv run run-dbt run
    uv run run-dbt run --select gold
    uv run run-dbt test
    uv run run-dbt debug
"""

import os
import sys

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings

logger = get_logger("cli.dbt")


def app() -> None:
    """Export Postgres settings as env vars, then exec dbt with local target."""
    if settings.postgres_user is None or settings.postgres_password is None:
        logger.critical(
            "Postgres credentials missing",
            suggestion="Create secrets/postgres_root_username and secrets/postgres_root_password",
        )
        raise SystemExit(1)

    # Bridge pydantic-settings → dbt env_var(): export resolved values so that
    # profiles.yml can read them via {{ env_var('POSTGRES_HOST') }} etc.
    os.environ["POSTGRES_HOST"] = settings.postgres_host
    os.environ["POSTGRES_PORT"] = str(settings.postgres_port)
    os.environ["POSTGRES_DB_NAME"] = settings.postgres_db_name
    os.environ["POSTGRES_USER"] = settings.postgres_user
    os.environ["POSTGRES_PASSWORD"] = settings.postgres_password.get_secret_value()

    os.execvp(
        "dbt",
        [
            "dbt",
            *sys.argv[1:],
            "--project-dir",
            str(settings.dbt_project_dir),
            "--profiles-dir",
            str(settings.dbt_project_dir),
        ],
    )


if __name__ == "__main__":
    app()
