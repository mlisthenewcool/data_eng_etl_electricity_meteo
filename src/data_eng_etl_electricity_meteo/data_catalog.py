"""Declarative YAML-based configuration for dataset sources and ingestion.

Architecture
------------

DataCatalog
    └── datasets: dict[str, DatasetConfig]
            ├── RemoteDatasetConfig (HTTP source)
            │   ├── name, description
            │   ├── source: RemoteSourceConfig (url, provider, format)
            │   └── ingestion: IngestionPolicy (frequency, mode)
            └── DerivedDatasetConfig (Gold, built from Silver)
                ├── name, description
                └── source: DerivedSourceConfig (depends_on)

Path resolution is handled by ``RemotePathResolver`` / ``DerivedPathResolver``,
see ``path_resolver.py``.
"""

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Self

import yaml
from pydantic import (
    Discriminator,
    HttpUrl,
    Tag,
    ValidationError,
    model_validator,
)

from data_eng_etl_electricity_meteo.core.exceptions import (
    AirflowContextError,
    DatasetNotFoundError,
    InvalidCatalogError,
)
from data_eng_etl_electricity_meteo.core.pydantic_base import StrictModel, format_pydantic_errors
from data_eng_etl_electricity_meteo.core.settings import settings

__all__: list[str] = [
    "SourceFormat",
    "IngestionFrequency",
    "IngestionMode",
    "IngestionPolicy",
    "RemoteSourceConfig",
    "DerivedSourceConfig",
    "RemoteDatasetConfig",
    "DerivedDatasetConfig",
    "DatasetConfig",
    "DataCatalog",
]


class SourceFormat(StrEnum):
    """Supported source file formats (``7z``, ``parquet``, ``json``)."""

    SEVEN_Z = "7z"
    PARQUET = "parquet"
    JSON = "json"

    @property
    def is_archive(self) -> bool:
        """Check if the format is a compressed archive."""
        return self == SourceFormat.SEVEN_Z


class IngestionFrequency(StrEnum):
    """Data update frequency, coupled with its Airflow schedule expression.

    Each member stores ``(value, airflow_schedule)`` so that adding a new
    frequency always requires defining its schedule.

    Examples
    --------
    >>> freq = IngestionFrequency.DAILY
    >>> freq.value
    'daily'
    >>> freq.airflow_schedule
    '@daily'
    """

    # Each member is defined as (value, airflow_schedule)
    # The tuple is unpacked in __new__ to set both attributes
    HOURLY = ("hourly", "@hourly")
    DAILY = ("daily", "@daily")
    WEEKLY = ("weekly", "@weekly")
    MONTHLY = ("monthly", "@monthly")
    YEARLY = ("yearly", "@yearly")
    NEVER = ("never", None)

    def __new__(cls, value: str, airflow_schedule: str | None):
        """Create enum member storing *value* and *airflow_schedule*."""
        obj = str.__new__(cls, value)
        obj._value_ = value
        # Store schedule in private attribute (dynamic attribute on enum)
        obj._airflow_schedule = airflow_schedule
        return obj

    @property
    def airflow_schedule(self) -> str | None:
        """Airflow cron preset (e.g. ``@daily``) or ``None``."""
        # Use getattr to satisfy type checkers (set dynamically in __new__)
        return getattr(self, "_airflow_schedule", None)

    def get_airflow_version_template(self, no_dash: bool = False) -> str:
        """Return the Airflow Jinja template for versioning.

        Hourly -> ``{{ ts }}`` / ``{{ ts_nodash }}``,
        otherwise -> ``{{ ds }}`` / ``{{ ds_nodash }}``.

        Parameters
        ----------
        no_dash:
            If ``True``, return the ``_nodash`` variant.

        Returns
        -------
        str
            Airflow template string resolved at runtime.

        Raises
        ------
        AirflowContextError
            If not running inside Airflow.
        """
        if not settings.is_running_on_airflow:
            raise AirflowContextError(
                operation="get_airflow_version_template",
                expected_context="Airflow runtime",
                suggestion="use format_datetime_as_version instead",
            )

        if self == IngestionFrequency.HOURLY:
            # TODO: "{{ data_interval_start.strftime('%Y%m%dT%H') }}"
            #  ou "{{ ts_nodash[:11] }}"
            return "{{ ts_nodash }}" if no_dash else "{{ ts }}"

        return "{{ ds_nodash }}" if no_dash else "{{ ds }}"

    def format_datetime_as_version(self, dt: datetime, no_dash: bool = False) -> str:
        """Format *dt* as a version string (non-Airflow contexts).

        Parameters
        ----------
        dt:
            Datetime to format.
        no_dash:
            If ``True``, return compact format without separators.

        Returns
        -------
        str
            Hourly: ``%Y%m%dT%H%M%S`` / ``%Y-%m-%d-T-%H-%M-%S``.
            Others: ``%Y%m%d`` / ``%Y-%m-%d``.
        """
        if self == IngestionFrequency.HOURLY:
            return dt.strftime("%Y%m%dT%H%M%S" if no_dash else "%Y-%m-%d-T-%H-%M-%S")

        # Daily/Weekly/Monthly/Yearly/Never
        return dt.strftime("%Y%m%d" if no_dash else "%Y-%m-%d")


# =============================================================================
# Source configs (split by dataset type)
# =============================================================================


class RemoteSourceConfig(StrictModel):
    """Source configuration for remote (HTTP) datasets.

    Attributes
    ----------
    provider:
        Data provider name (e.g. ``"IGN"``).
    url:
        Download URL.
    format:
        File format.
    inner_file:
        File to extract from archive (required iff ``format.is_archive``).
    """

    provider: str
    url: HttpUrl
    format: SourceFormat
    inner_file: str | None = None

    @model_validator(mode="after")
    def validate_inner_file_consistency(self) -> Self:
        """Ensure ``inner_file`` is set iff format is an archive."""
        if self.format.is_archive and self.inner_file is None:
            raise ValueError(f"inner_file is required for archive format: {self.format}")
        if not self.format.is_archive and self.inner_file is not None:
            raise ValueError(f"inner_file must not be set for non-archive format: {self.format}")
        return self

    @property
    def url_as_str(self) -> str:
        """Return ``url`` as ``str``."""
        return str(self.url)


class DerivedSourceConfig(StrictModel):
    """Source configuration for derived (Gold) datasets.

    Attributes
    ----------
    depends_on:
        Silver dataset names this Gold dataset is built from.
    """

    depends_on: list[str]

    @model_validator(mode="after")
    def validate_depends_on_not_empty(self) -> Self:
        """Ensure ``depends_on`` has at least one entry."""
        if not self.depends_on:
            raise ValueError("depends_on must contain at least one dataset name")
        return self


# =============================================================================
# Ingestion policy
# =============================================================================


class IngestionMode(StrEnum):
    """Ingestion mode (``snapshot`` or ``incremental``)."""

    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"


class IngestionPolicy(StrictModel):
    """Ingestion frequency and mode for a dataset.

    Attributes
    ----------
    frequency:
        Update frequency (also determines version format).
    mode:
        Snapshot or incremental.
    """

    frequency: IngestionFrequency
    mode: IngestionMode
    # incremental_key: str  # TODO: required if mode=INCREMENTAL


# =============================================================================
# Dataset configs (split by dataset type) + discriminated union
# =============================================================================


class RemoteDatasetConfig(StrictModel):
    """Remote (HTTP) dataset entry.

    Attributes
    ----------
    name:
        Dataset identifier (auto-injected from YAML key).
    description:
        Human-readable description.
    source:
        Remote source (url, provider, format).
    ingestion:
        Ingestion frequency and mode.
    """

    name: str
    description: str
    source: RemoteSourceConfig
    ingestion: IngestionPolicy


class DerivedDatasetConfig(StrictModel):
    """Derived (Gold) dataset entry built from Silver sources.

    Attributes
    ----------
    name:
        Dataset identifier (auto-injected from YAML key).
    description:
        Human-readable description.
    source:
        Derived source (depends_on).
    """

    name: str
    description: str
    source: DerivedSourceConfig


def _dataset_discriminator(v: Any) -> str:
    """Discriminate between remote and derived datasets.

    Handles both raw ``dict`` (deserialization) and model instances
    (serialization), as required by Pydantic.
    """
    if isinstance(v, dict):
        source = v.get("source", {})
        if isinstance(source, dict) and "url" in source:
            return "remote"
        return "derived"
    if isinstance(v, RemoteDatasetConfig):
        return "remote"
    return "derived"


type DatasetConfig = Annotated[
    Annotated[RemoteDatasetConfig, Tag("remote")] | Annotated[DerivedDatasetConfig, Tag("derived")],
    Discriminator(_dataset_discriminator),
]


# =============================================================================
# Catalog
# =============================================================================


class DataCatalog(StrictModel):
    """Root catalog loaded from ``data_catalog.yaml``.

    Attributes
    ----------
    datasets:
        Mapping of dataset name to its configuration.
    """

    datasets: dict[str, DatasetConfig]

    @model_validator(mode="before")
    @classmethod
    def inject_names_into_datasets(cls, data: dict) -> dict:
        """Inject the YAML key into the 'name' field of each dataset."""
        if isinstance(data, dict) and "datasets" in data:
            for name, config in data["datasets"].items():
                if isinstance(config, dict):
                    config["name"] = name
        return data

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load and validate the catalog from a YAML file.

        Parameters
        ----------
        path:
            Path to the YAML catalog file.

        Returns
        -------
        DataCatalog
            Validated catalog instance.

        Raises
        ------
        InvalidCatalogError
            File missing, invalid YAML, or Pydantic validation failure.
        """
        if not path.exists():
            raise InvalidCatalogError(path=path, reason="file doesn't exist")

        try:
            with path.open() as f:
                data = yaml.safe_load(f)

            if data is None:
                raise InvalidCatalogError(
                    path=path,
                    reason="catalog file is empty or contains only comments",
                )

            return cls.model_validate(data)

        except yaml.YAMLError as yaml_error:
            raise InvalidCatalogError(
                path=path,
                reason=f"error parsing YAML: {yaml_error}",
            ) from yaml_error
        except ValidationError as pydantic_errors:
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation errors",
                validation_errors=format_pydantic_errors(pydantic_errors),
            ) from None

    def get_dataset(self, name: str) -> RemoteDatasetConfig | DerivedDatasetConfig:
        """Retrieve a dataset by name.

        Parameters
        ----------
        name:
            Dataset identifier.

        Returns
        -------
        RemoteDatasetConfig | DerivedDatasetConfig

        Raises
        ------
        DatasetNotFoundError
            If *name* is not in the catalog.
        """
        dataset = self.datasets.get(name)

        if not dataset:
            raise DatasetNotFoundError(
                name=name,
                available_datasets=list(self.datasets.keys()),
            )

        return dataset

    def get_remote_datasets(self) -> list[RemoteDatasetConfig]:
        """Return all remote (HTTP) datasets."""
        return [d for d in self.datasets.values() if isinstance(d, RemoteDatasetConfig)]

    def get_derived_datasets(self) -> list[DerivedDatasetConfig]:
        """Return all derived (Gold) datasets."""
        return [d for d in self.datasets.values() if isinstance(d, DerivedDatasetConfig)]

    @model_validator(mode="after")
    def validate_derived_dependencies_exist(self) -> Self:
        """Ensure ``depends_on`` entries exist and are remote datasets."""
        for dataset in self.get_derived_datasets():
            for dep_name in dataset.source.depends_on:
                if dep_name not in self.datasets:
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on "
                        f"'{dep_name}' which is not defined "
                        f"in the catalog"
                    )

                dep = self.datasets[dep_name]
                if not isinstance(dep, RemoteDatasetConfig):
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on "
                        f"'{dep_name}' which is itself derived. "
                        f"Gold datasets can only depend on "
                        f"Silver datasets (from remote sources)."
                    )
        return self


if __name__ == "__main__":
    import sys

    from data_eng_etl_electricity_meteo.core.logger import logger

    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(log_method=logger.critical)
        sys.exit(-1)

    logger.info(f"--- Catalog loaded: {len(catalog.datasets)} dataset(s) ---")

    for _name, _dataset in catalog.datasets.items():
        logger.info(_name, **_dataset.model_dump(exclude={"name"}))
