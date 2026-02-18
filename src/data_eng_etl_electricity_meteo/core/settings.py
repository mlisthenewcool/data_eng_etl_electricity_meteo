"""Centralized application settings (Pydantic). Override via environment variables."""

from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import DirectoryPath, Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(StrEnum):
    """Standard logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Immutable application settings. Override via .env file."""

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

    # @computed_field
    # @property
    # def is_running_in_notebook(self) -> bool:
    #     """Detect if running inside an active Marimo notebook.
    #
    #     This property is evaluated at each call (not cached) to ensure it correctly
    #     detects the runtime state after kernel initialization.
    #
    #     Returns
    #     -------
    #     bool
    #         True if running in Marimo (edit/run mode).
    #         False otherwise (terminal, script, tests).
    #     """
    #     try:
    #         import marimo as mo  # noqa: PLC0415
    #
    #         if mo.running_in_notebook():
    #             return True
    #     except ImportError:
    #         return False
    #     return False

    # =========================================================================
    # Paths (computed from root_dir, not configurable via env)
    # =========================================================================
    root_dir: Path = Field(
        default=Path(__file__).resolve().parent.parent.parent.parent,
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

    # @computed_field
    # @property
    # def secrets_dir_path(self) -> DirectoryPath:
    #     """Path to secrets directory (Docker secrets on Airflow, local otherwise)."""
    #     if self.is_running_on_airflow:
    #         return Path("/run/secrets")
    #
    #     return self.root_dir / "secrets"

    # =========================================================================
    # Download Settings
    # =========================================================================
    download_chunk_size: int = Field(
        default=1024 * 1024,  # 1 MB
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
        default=1024 * 128,  # 128 KB
        description="Chunk size for file hashing (bytes)",
        gt=0,
        le=1024 * 1024,  # Max 1 MB
    )

    # =========================================================================
    # Pydantic Config
    # =========================================================================
    model_config = SettingsConfigDict(
        # env_prefix="ENV_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,  # env vars are defined in capitals
        extra="ignore",  # 'forbid' crashes because of PYTHONPATH
        frozen=True,  # Immutable settings
    )


# Singleton instance
settings = Settings()
