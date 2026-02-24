"""Centralized application settings (Pydantic). Override via environment variables."""

from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Literal, Self

from pydantic import AliasChoices, DirectoryPath, Field, SecretStr, computed_field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SecretsSettingsSource,
    SettingsConfigDict,
)


class LogLevel(StrEnum):
    """Standard logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Immutable application settings loaded from env vars and Docker secrets.

    Credential resolution (``postgres_user`` / ``postgres_password``) follows
    pydantic-settings' standard source priority chain:

    1. Environment variables (``POSTGRES_USER`` / ``POSTGRES_PASSWORD``).
    2. Docker secrets files in ``_SECRETS_DIR``
       (``postgres_root_username`` / ``postgres_root_password``).

    This means the loader always reads ``settings.postgres_user`` and
    ``settings.postgres_password`` uniformly, regardless of environment.
    """

    # Secrets directory resolved at class-definition time:
    # - Docker : /run/secrets  (Docker secrets mount)
    # - Local  : {project_root}/secrets/  (local copies without .txt extension)
    _SECRETS_DIR: ClassVar[Path] = (
        Path("/run/secrets")
        if Path("/run/secrets").exists()
        else Path(__file__).resolve().parents[3] / "secrets"
    )

    # =========================================================================
    # General config
    # =========================================================================
    logging_level: LogLevel = Field(default=LogLevel.INFO, description="The logger verbosity level")

    # =========================================================================
    # Data config
    # =========================================================================
    bronze_retention_days: int = Field(
        default=365,  # 1 year
        description="Number of days to retain bronze layer versions",
        gt=0,
        le=365 * 3,  # Max 3 years
    )

    # =========================================================================
    # PostgreSQL connection
    # =========================================================================
    postgres_host: str = Field(default="localhost", description="PostgreSQL host")
    postgres_port: int = Field(default=5432, description="PostgreSQL port", gt=0, le=65535)
    project_db_name: str = Field(default="project", description="Project database name")

    # AliasChoices accepts both the env-var name and the Docker secrets filename.
    # Local dev : set POSTGRES_USER / POSTGRES_PASSWORD env vars.
    # Docker    : SecretsSettingsSource reads postgres_root_{username,password}.
    postgres_user: str | None = Field(
        default=None,
        validation_alias=AliasChoices("postgres_user", "postgres_root_username"),
        description="PostgreSQL user.",
    )
    postgres_password: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("postgres_password", "postgres_root_password"),
        description="PostgreSQL password.",
    )

    # =========================================================================
    # Airflow config
    # =========================================================================
    airflow_home: Path = Field(
        default=Path("/opt/airflow"),
        # validation_alias="AIRFLOW_HOME",  # the variable doesn't have the ENV prefix
        description="The Airflow home directory. Uses standard AIRFLOW_HOME env var",
    )

    @computed_field
    @property
    def is_running_on_airflow(self) -> bool:
        """Check if running inside Airflow by detecting airflow.cfg in AIRFLOW_HOME."""
        # TODO: est-ce aussi valable en mode 'worker' ou 'scheduler' ?
        #   remplacer par les env vars AIRFLOW_CONFIG ou AIRFLOW_HOME par exemple
        return (self.airflow_home / "airflow.cfg").exists()

    # =========================================================================
    # Paths (computed from root_dir, not configurable via env)
    # =========================================================================
    root_dir: Path = Field(
        default=Path(__file__).resolve().parents[3],
        description="Project root directory (computed, not configurable)",
        exclude=True,  # Don't expose in env vars
    )

    @computed_field
    @property
    def data_dir_path(self) -> DirectoryPath:
        """Data directory (computed from root_dir)."""
        return self.root_dir / "data"

    @computed_field
    @property
    def data_catalog_file_path(self) -> Path:
        """Path to data catalog YAML file."""
        return self.data_dir_path / "catalog.yaml"

    @computed_field
    @property
    def data_state_dir_path(self) -> DirectoryPath:
        """Path to pipeline state directory."""
        return self.data_dir_path / "_state"

    # =========================================================================
    # Download Settings
    # =========================================================================
    download_chunk_size: int = Field(
        # default=1024 * 1024,  # 1 MB
        default=512 * 1024,  # 512 KB
        description="Chunk size for streaming downloads (bytes)",
        gt=0,
        le=10 * 1024 * 1024,  # Max 10 MB
    )

    download_timeout_total: int = Field(
        default=600,
        description="Maximum time for entire download (seconds)",
        gt=0,
        le=3600,  # Max 1 hour
    )

    download_timeout_connect: int = Field(
        default=10,
        description="Maximum time to establish connection (seconds)",
        gt=0,
        le=60,
    )

    download_timeout_sock_read: int = Field(
        default=30,
        description="Maximum time between data packets (seconds)",
        gt=0,
        le=300,
    )

    @model_validator(mode="after")
    def validate_timeout_hierarchy(self) -> Self:
        """Ensure total timeout exceeds connect and read timeouts."""
        if self.download_timeout_total <= self.download_timeout_connect:
            raise ValueError("download_timeout_total must be > download_timeout_connect")

        if self.download_timeout_total <= self.download_timeout_sock_read:
            raise ValueError("download_timeout_total must be > download_timeout_sock_read")

        return self

    # =========================================================================
    # Hash Settings
    # =========================================================================
    hash_algorithm: Literal["sha256", "sha512", "sha1", "md5"] = Field(
        default="sha256",
        description="Hashing algorithm for integrity checks (recommended: sha256)",
    )

    hash_chunk_size: int = Field(
        default=128 * 1024,  # 128 KB
        description="Chunk size for file hashing (bytes)",
        gt=0,
        le=1024 * 1024,  # Max 1 MB
    )

    # =========================================================================
    # Pydantic Config + source chain
    # =========================================================================
    model_config = SettingsConfigDict(
        # env_prefix="ENV_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,  # env vars are defined in capitals
        extra="ignore",  # 'forbid' crashes because of PYTHONPATH
        frozen=True,  # Immutable settings
    )

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

        The built-in ``file_secret_settings`` (which reads from
        ``model_config['secrets_dir']``) is intentionally replaced by an
        explicit ``SecretsSettingsSource`` so the path is a class constant
        rather than a configurable setting.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            SecretsSettingsSource(settings_cls, secrets_dir=cls._SECRETS_DIR),
        )


# Singleton instance
settings = Settings()
