"""Declarative YAML-based configuration for dataset sources and ingestion.

Architecture
------------

DataCatalog
    └── datasets: dict[str, DatasetConfig]
            ├── RemoteDatasetConfig (HTTP source)
            │   ├── name, description
            │   ├── source: RemoteSourceConfig (url, provider, format)
            │   └── ingestion: IngestionPolicy (frequency, mode)
            └── GoldDatasetConfig (Gold, built from Silver)
                ├── name, description
                └── source: GoldSourceConfig (depends_on)

Path resolution is handled by ``RemotePathResolver``, see ``path_resolver.py``.
Gold datasets live in Postgres (via dbt), not on disk.
"""

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Self

import yaml
from pydantic import Discriminator, HttpUrl, Tag, ValidationError, model_validator

from data_eng_etl_electricity_meteo.core.exceptions import (
    AirflowContextError,
    DatasetNotFoundError,
    DatasetTypeError,
    InvalidCatalogError,
)
from data_eng_etl_electricity_meteo.core.pydantic_base import StrictModel, format_pydantic_errors
from data_eng_etl_electricity_meteo.core.settings import settings


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

    Examples
    --------
    >>> freq = IngestionFrequency.DAILY
    >>> freq.value
    'daily'
    >>> freq.airflow_schedule
    '@daily'
    """

    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    NEVER = "never"

    @property
    def airflow_schedule(self) -> str | None:
        """Airflow cron preset (e.g. ``@daily``) or ``None``."""
        return _AIRFLOW_SCHEDULES[self]

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
            #  or "{{ ts_nodash[:11] }}"
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
            ``no_dash=False`` (default): ``%Y-%m-%d`` (daily+)
            or ``%Y-%m-%d-T-%H-%M-%S`` (hourly).
            ``no_dash=True``: ``%Y%m%d`` (daily+)
            or ``%Y%m%dT%H%M%S`` (hourly).
        """
        if self == IngestionFrequency.HOURLY:
            return dt.strftime("%Y%m%dT%H%M%S" if no_dash else "%Y-%m-%d-T-%H-%M-%S")

        # All non-hourly frequencies are date-only — no time component needed.
        return dt.strftime("%Y%m%d" if no_dash else "%Y-%m-%d")


_AIRFLOW_SCHEDULES: dict[IngestionFrequency, str | None] = {
    IngestionFrequency.HOURLY: "@hourly",
    IngestionFrequency.DAILY: "@daily",
    IngestionFrequency.WEEKLY: "@weekly",
    IngestionFrequency.MONTHLY: "@monthly",
    IngestionFrequency.YEARLY: "@yearly",
    IngestionFrequency.NEVER: None,
}


# ---------------------------------------------------------------------------
# Source configs (split by dataset type)
# ---------------------------------------------------------------------------


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


class GoldSourceConfig(StrictModel):
    """Source configuration for Gold datasets.

    ``depends_on`` is the single source of truth for Gold dependencies.
    The future dbt ``sources.yml`` will be generated from the catalog,
    not maintained separately.

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


# ---------------------------------------------------------------------------
# Ingestion policy
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Dataset configs (split by dataset type) + discriminated union
# ---------------------------------------------------------------------------


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


class GoldDatasetConfig(StrictModel):
    """Gold dataset entry built from Silver sources.

    Attributes
    ----------
    name:
        Dataset identifier (auto-injected from YAML key).
    description:
        Human-readable description.
    source:
        Gold source (depends_on).
    """

    name: str
    description: str
    source: GoldSourceConfig


def _dataset_discriminator(
    v: dict[str, object] | RemoteDatasetConfig | GoldDatasetConfig,
) -> str:
    """Discriminate between remote and gold datasets.

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
    Annotated[RemoteDatasetConfig, Tag("remote")] | Annotated[GoldDatasetConfig, Tag("derived")],
    Discriminator(_dataset_discriminator),
]


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


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
    def inject_names_into_datasets(cls, data: Any) -> Any:
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
            # Suppress pydantic internals from traceback (raise ... from None);
            # details are captured in validation_errors.
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation errors",
                validation_errors=format_pydantic_errors(pydantic_errors),
            ) from None

    def get_remote_dataset(self, name: str) -> RemoteDatasetConfig:
        """Retrieve a remote dataset by name.

        Parameters
        ----------
        name:
            Dataset identifier.

        Returns
        -------
        RemoteDatasetConfig

        Raises
        ------
        DatasetNotFoundError
            If *name* is not in the catalog.
        DatasetTypeError
            If *name* refers to a gold dataset.
        """
        dataset = self.datasets.get(name)
        if dataset is None:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))
        if not isinstance(dataset, RemoteDatasetConfig):
            raise DatasetTypeError(
                name=name, expected="RemoteDatasetConfig", actual=type(dataset).__name__
            )
        return dataset

    def get_gold_dataset(self, name: str) -> GoldDatasetConfig:
        """Retrieve a gold dataset by name.

        Parameters
        ----------
        name:
            Dataset identifier.

        Returns
        -------
        GoldDatasetConfig

        Raises
        ------
        DatasetNotFoundError
            If *name* is not in the catalog.
        DatasetTypeError
            If *name* refers to a remote dataset.
        """
        dataset = self.datasets.get(name)
        if dataset is None:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))
        if not isinstance(dataset, GoldDatasetConfig):
            raise DatasetTypeError(
                name=name, expected="GoldDatasetConfig", actual=type(dataset).__name__
            )
        return dataset

    def get_remote_datasets(self) -> list[RemoteDatasetConfig]:
        """Return all remote (HTTP) datasets."""
        return [d for d in self.datasets.values() if isinstance(d, RemoteDatasetConfig)]

    def get_gold_datasets(self) -> list[GoldDatasetConfig]:
        """Return all gold datasets."""
        return [d for d in self.datasets.values() if isinstance(d, GoldDatasetConfig)]

    @model_validator(mode="after")
    def validate_gold_dependencies_exist(self) -> Self:
        """Ensure ``depends_on`` entries exist and are remote datasets."""
        for dataset in self.get_gold_datasets():
            for dep_name in dataset.source.depends_on:
                if dep_name not in self.datasets:
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on "
                        f"'{dep_name}' which is not defined in the catalog"
                    )

                dep = self.datasets[dep_name]
                if not isinstance(dep, RemoteDatasetConfig):
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on '{dep_name}'"
                        " which is itself a gold dataset."
                        " Gold datasets can only depend on Silver datasets."
                    )
        return self


if __name__ == "__main__":
    from data_eng_etl_electricity_meteo.core.logger import get_logger

    _logger = get_logger("data_catalog")

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(_logger.critical)
        raise SystemExit(1)

    _logger.info("Catalog loaded", dataset_count=len(_catalog.datasets))

    for _name, _dataset in _catalog.datasets.items():
        _logger.info("Dataset entry", dataset_name=_name, **_dataset.model_dump(exclude={"name"}))
