"""Centralized application settings (Pydantic). Override via environment variables."""

import os
import sys
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from typing import Annotated, ClassVar

from pydantic import (
    BeforeValidator,
    DirectoryPath,
    Field,
    SecretStr,
    ValidationError,
    computed_field,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SecretsSettingsSource,
    SettingsConfigDict,
)


class LogLevel(StrEnum):
    """Standard logging levels (lowercase to match structlog's naming convention)."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Settings(BaseSettings):
    """Immutable application settings loaded from env vars and Docker secrets.

    Frozen (immutable) after instantiation.
    All computed fields are cached on first access via ``@cached_property``.

    Source priority chain (highest to lowest):

    1. Values passed at instantiation (useful for tests).
    2. Environment variables.
    3. ``.env`` file (``_ROOT_DIR / ".env"``).
    4. Docker secrets files in ``_SECRETS_DIR``.

    Notes
    -----
    Credentials (``postgres_user`` / ``postgres_password``) are resolved from secrets
    files via ``SecretsSettingsSource``. Env vars under the alias names
    (``POSTGRES_ROOT_USERNAME`` / ``POSTGRES_ROOT_PASSWORD``) are technically accepted
    by pydantic-settings but are not documented or supported.

    Computed path fields typed as ``DirectoryPath``
    (``data_dir_path``, ``postgres_dir_path``) validate that the target directory exists
    on first access. Access will fail if those directories are absent.
    """

    # -- Class internals ---------------------------------------------------------------

    # Project root resolved at class-definition time (before Pydantic
    # processes Field() defaults). Used by _SECRETS_DIR and model_config.
    _ROOT_DIR: ClassVar[Path] = Path(__file__).resolve().parents[3]

    # Secrets directory resolved at class-definition time:
    # - Docker : /run/secrets  (Docker secrets mount)
    # - Local  : {project_root}/secrets/
    _SECRETS_DIR: ClassVar[Path] = (
        Path("/run/secrets") if Path("/run/secrets").exists() else _ROOT_DIR / "secrets"
    )

    # -- Pydantic settings -------------------------------------------------------------

    model_config = SettingsConfigDict(
        env_file=_ROOT_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,  # env vars are defined in capitals
        extra="ignore",  # .env contains AIRFLOW_DB_NAME (Docker Compose only, not a field)
        frozen=True,  # Immutable settings
    )

    # -- General settings --------------------------------------------------------------

    logging_level: Annotated[
        LogLevel, BeforeValidator(lambda v: v.lower() if isinstance(v, str) else v)
    ] = Field(default=LogLevel.INFO, description="The logger verbosity level")

    # -- Data settings -----------------------------------------------------------------

    bronze_retention_days: int = Field(
        default=365,  # 1 year
        description="Number of days to retain bronze layer versions",
        gt=0,
        le=365 * 3,  # Max 3 years
    )

    # -- Postgres connection -----------------------------------------------------------
    # Airflow connection id: see loaders/pg_connection.py::AIRFLOW_CONN_ID.

    postgres_host: str = Field(default="localhost", description="Postgres host")
    postgres_port: int = Field(default=5432, description="Postgres port", gt=0, le=65535)
    postgres_db_name: str = Field(default="electricity_meteo", description="Postgres database name")

    # Credentials are resolved from secrets files (secrets/ directory).
    # SecretsSettingsSource matches the validation_alias to the filename.
    # Env vars under the alias names (POSTGRES_ROOT_USERNAME / POSTGRES_ROOT_PASSWORD)
    # are technically accepted by pydantic-settings but not documented or supported.
    postgres_user: str | None = Field(
        default=None,
        validation_alias="postgres_root_username",
        description="Postgres user (from secrets file)",
    )
    postgres_password: SecretStr | None = Field(
        default=None,
        validation_alias="postgres_root_password",
        description="Postgres password (from secrets file)",
    )

    # -- Airflow settings --------------------------------------------------------------

    @computed_field
    @cached_property
    def is_running_on_airflow(self) -> bool:
        """Detect Airflow by checking the ``AIRFLOW_HOME`` environment variable.

        ``AIRFLOW_HOME`` is always set by Airflow in every context
        (standalone, scheduler, worker, task subprocess).
        """
        return "AIRFLOW_HOME" in os.environ

    # -- Paths (derived from _ROOT_DIR, not configurable via env) ----------------------
    # DirectoryPath (Pydantic) validates that the directory exists at access time.
    # Use plain Path for directories that may not exist yet.

    @computed_field
    @cached_property
    def data_dir_path(self) -> DirectoryPath:
        """Data directory (computed from ``_ROOT_DIR``)."""
        return self._ROOT_DIR / "data"

    @computed_field
    @cached_property
    def data_catalog_file_path(self) -> Path:
        """Path to data catalog YAML file."""
        return self.data_dir_path / "catalog.yaml"

    @computed_field
    @cached_property
    def data_state_dir_path(self) -> Path:
        """Path to pipeline state directory (created on first write)."""
        return self.data_dir_path / "_state"

    @computed_field
    @cached_property
    def postgres_dir_path(self) -> DirectoryPath:
        """Path to Postgres' queries and configuration directory."""
        return self._ROOT_DIR / "postgres"

    # -- dbt settings ------------------------------------------------------------------

    @computed_field
    @cached_property
    def dbt_project_dir(self) -> DirectoryPath:
        """Path to the dbt project directory (computed from ``_ROOT_DIR``)."""
        return self._ROOT_DIR / "dbt"

    @computed_field
    @cached_property
    def dbt_log_path(self) -> Path:
        """Path to the dbt log output directory."""
        return self.dbt_project_dir / "logs"

    @computed_field
    @cached_property
    def dbt_target_path(self) -> Path:
        """Path to the dbt target (compiled artifacts) directory."""
        return self.dbt_project_dir / "target"

    # -- Download settings -------------------------------------------------------------

    download_timeout_seconds: int = Field(
        default=600,
        description="Maximum total time for a single file download",
        gt=0,
        le=3600,  # Max 1 hour
    )

    # -- Source chain ------------------------------------------------------------------

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define the settings source chain.

        Sources are listed in decreasing priority:

        1. ``init_settings``    — values passed at instantiation (tests, overrides)
        2. ``env_settings``     — environment variables
        3. ``dotenv_settings``  — ``.env`` file
        4. ``SecretsSettingsSource`` — Docker secrets files (``_SECRETS_DIR``)

        The built-in ``file_secret_settings``
        (which reads from ``model_config['secrets_dir']``) is intentionally replaced by
        an explicit ``SecretsSettingsSource`` so the path is a class constant rather
        than a configurable setting.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            SecretsSettingsSource(settings_cls, secrets_dir=cls._SECRETS_DIR),
        )


# --------------------------------------------------------------------------------------
# Module-level singleton
# --------------------------------------------------------------------------------------


def _load_settings() -> Settings:
    """Instantiate settings, aborting with a clear message on validation error.

    Uses ``print(…, file=stderr)`` instead of structlog because the project logger
    depends on ``settings.logging_level`` (circular), and calling
    ``structlog.configure()`` here would overwrite Airflow's own config.
    """
    try:
        return Settings()
    except ValidationError as exc:
        print("FATAL — Invalid settings (Pydantic validation errors)", file=sys.stderr)
        for error in exc.errors():
            field = error["loc"][-1]
            msg = error["msg"]
            value = error.get("input")
            print(f"\t* {field}={value!r} — {msg}", file=sys.stderr)
        raise SystemExit(1)


settings = _load_settings()
