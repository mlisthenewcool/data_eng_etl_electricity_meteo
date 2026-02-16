"""Declarative YAML-based configuration for dataset sources and ingestion.

Architecture
------------

DataCatalog
    └── datasets: dict[str, DatasetConfig]
        ├── name (auto-injected from YAML key)
        ├── source: SourceConfig (remote HTTP or derived Gold)
        └── ingestion: IngestionPolicy (frequency, mode)

Path resolution is handled by ``PathResolver``, see ``path_resolver.py``.
"""

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Self

import yaml
from pydantic import (
    HttpUrl,
    ValidationError,
    model_validator,
)

from data_eng_etl_electricity_meteo.core.exceptions import (
    DatasetNotFoundError,
    InvalidCatalogError,
)
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.utils.pydantic_utils import StrictModel, format_pydantic_errors


class SourceFormat(StrEnum):
    """Supported source file formats (``7z``, ``parquet``, ``json``)."""

    SEVEN_Z = "7z"
    PARQUET = "parquet"
    JSON = "json"

    @property
    def is_archive(self) -> bool:
        """Check if the format is a compressed archive requiring extraction."""
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
        """Airflow cron preset (e.g. ``@daily``) or ``None`` for ``NEVER``."""
        # Use getattr to satisfy type checkers (attribute set dynamically in __new__)
        return getattr(self, "_airflow_schedule", None)

    def get_airflow_version_template(self, no_dash: bool = False) -> str:
        """Return the Airflow Jinja template for versioning.

        Hourly -> ``{{ ts }}`` / ``{{ ts_nodash }}``,
        otherwise -> ``{{ ds }}`` / ``{{ ds_nodash }}``.

        Parameters
        ----------
        no_dash : bool
            If ``True``, return the ``_nodash`` variant.

        Returns
        -------
        str
            Airflow template string resolved at runtime.

        Raises
        ------
        Exception
            If not running inside Airflow.
        """
        if not settings.is_running_on_airflow:
            # raise AirflowContextError(
            #     operation="`get_airflow_version_template`",
            #     expected_context="airflow",
            #     actual_context="not airflow",
            #     suggestion="use `format_datetime_as_version` instead",
            # )
            raise Exception("Not running on Airflow (check settings.is_running_on_airflow)")

        if self == IngestionFrequency.HOURLY:
            # TODO: "{{ data_interval_start.strftime('%Y%m%dT%H') }}"
            #  ou "{{ ts_nodash[:11] }}"
            return "{{ ts_nodash }}" if no_dash else "{{ ts }}"

        return "{{ ds_nodash }}" if no_dash else "{{ ds }}"

    def format_datetime_as_version(self, dt: datetime, no_dash: bool = False) -> str:
        """Format *dt* as a version string (non-Airflow contexts).

        Parameters
        ----------
        dt : datetime
            Datetime to format.
        no_dash : bool
            If ``True``, return compact format without separators.

        Returns
        -------
        str
            Hourly: ``%Y%m%dT%H%M%S`` / ``%Y-%m-%d-T-%H-%M-%S``.
            Others: ``%Y%m%d`` / ``%Y-%m-%d``.
        """
        if settings.is_running_on_airflow:
            # raise AirflowContextError(
            #     operation="`format_datetime_as_version`",
            #     expected_context="not airflow",
            #     actual_context="airflow",
            #     suggestion="use `get_airflow_version_template` instead",
            # )
            pass

        if self == IngestionFrequency.HOURLY:
            return dt.strftime("%Y%m%dT%H%M%S" if no_dash else "%Y-%m-%d-T-%H-%M-%S")

        # Daily/Weekly/Monthly/Yearly/Never
        return dt.strftime("%Y%m%d" if no_dash else "%Y-%m-%d")


class SourceConfig(StrictModel):
    """Dataset source configuration -- remote (HTTP) **or** derived (Gold).

    Mutually exclusive: must have either ``(url, provider, format)``
    or ``depends_on``, never both.

    Attributes
    ----------
    provider : str | None
        Data provider name (remote only, e.g. ``"IGN"``).
    url : HttpUrl | None
        Download URL (remote only).
    format : SourceFormat | None
        File format (remote only).
    inner_file : str | None
        File to extract from archive (required iff ``format.is_archive``).
    depends_on : list[str] | None
        Silver dataset names (derived only).
    """

    # Remote source fields
    provider: str | None = None
    url: HttpUrl | None = None
    format: SourceFormat | None = None
    inner_file: str | None = None

    # Derived source field
    depends_on: list[str] | None = None

    @model_validator(mode="after")
    def validate_source_consistency(self) -> Self:
        """Ensure remote / derived fields are mutually exclusive and complete."""
        has_remote = self.url is not None
        has_derived = self.depends_on is not None

        # Mutual exclusivity
        if has_remote and has_derived:
            raise ValueError(
                "source cannot have both remote fields (url) and derived fields (depends_on)"
            )

        if not has_remote and not has_derived:
            raise ValueError("source must have either url (remote) or depends_on (derived)")

        # Validate remote source completeness
        if has_remote:
            if self.provider is None:
                raise ValueError("provider is required for remote sources")
            if self.format is None:
                raise ValueError("format is required for remote sources")

            # Validate inner_file consistency with format
            if self.format.is_archive and self.inner_file is None:
                raise ValueError(f"inner_file is required for archive format: {self.format}")
            if not self.format.is_archive and self.inner_file is not None:
                raise ValueError(
                    f"inner_file must not be set for non-archive format: {self.format}"
                )

        # Validate derived source
        if has_derived:
            if not self.depends_on:
                raise ValueError("depends_on must contain at least one dataset name")
            if self.provider is not None or self.format is not None:
                raise ValueError(
                    "provider and format must not be set for derived sources (only depends_on)"
                )

        return self

    @property
    def is_remote(self) -> bool:
        """``True`` if remote source (has ``url``)."""
        return self.url is not None

    @property
    def is_derived(self) -> bool:
        """``True`` if derived source (has ``depends_on``)."""
        return self.depends_on is not None

    @property
    def url_as_str(self) -> str:
        """Return ``url`` as ``str``.

        Raises
        ------
        ValueError
            If called on a derived source.
        """
        if self.url is None:
            raise ValueError("url_as_str called on derived source (url is None)")
        return str(self.url)


class IngestionMode(StrEnum):
    """Ingestion mode (``snapshot`` or ``incremental``)."""

    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"


class IngestionPolicy(StrictModel):
    """Ingestion frequency and mode for a dataset.

    Attributes
    ----------
    frequency : IngestionFrequency
        Update frequency (also determines version format).
    mode : IngestionMode
        Snapshot or incremental.
    """

    frequency: IngestionFrequency
    mode: IngestionMode
    # incremental_key: str  # TODO: Add validation -> required if mode=INCREMENTAL


class DatasetConfig(StrictModel):
    """Single dataset entry -- remote (HTTP) or derived (Gold).

    Attributes
    ----------
    name : str
        Dataset identifier (auto-injected from YAML key).
    description : str
        Human-readable description.
    source : SourceConfig
        Remote or derived source configuration.
    ingestion : IngestionPolicy | None
        Required for remote, forbidden for derived.
    """

    name: str
    description: str
    source: SourceConfig
    ingestion: IngestionPolicy | None = None

    @model_validator(mode="after")
    def validate_ingestion_consistency(self) -> Self:
        """Ensure remote datasets have ingestion and derived ones do not."""
        if self.source.is_remote and self.ingestion is None:
            raise ValueError(
                f"Dataset '{self.name}': ingestion is required for remote datasets "
                f"(source has url={self.source.url})"
            )

        if self.source.is_derived and self.ingestion is not None:
            raise ValueError(
                f"Dataset '{self.name}': ingestion must not be set for derived datasets "
                f"(source has depends_on={self.source.depends_on})"
            )

        return self

    @property
    def is_remote(self) -> bool:
        """``True`` if remote dataset."""
        return self.source.is_remote

    @property
    def is_derived(self) -> bool:
        """``True`` if derived dataset."""
        return self.source.is_derived


class DataCatalog(StrictModel):
    """Root catalog loaded from ``data_catalog.yaml``.

    Attributes
    ----------
    datasets : dict[str, DatasetConfig]
        Mapping of dataset name to its configuration.
    """

    datasets: dict[str, DatasetConfig]

    @model_validator(mode="before")
    @classmethod
    def inject_names_into_datasets(cls, data: dict) -> dict:
        """Inject the YAML dictionary key into the 'name' field of each dataset."""
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
        path : Path
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
                    path=path, reason="catalog file is empty or contains only comments"
                )

            return cls.model_validate(data)

        except yaml.YAMLError as yaml_error:
            raise InvalidCatalogError(
                path=path, reason=f"error parsing YAML: {yaml_error}"
            ) from yaml_error
        except ValidationError as pydantic_errors:
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation errors",
                validation_errors=format_pydantic_errors(pydantic_errors),
            ) from None

    def get_dataset(self, name: str) -> DatasetConfig:
        """Retrieve a dataset by name.

        Parameters
        ----------
        name : str
            Dataset identifier.

        Returns
        -------
        DatasetConfig

        Raises
        ------
        DatasetNotFoundError
            If *name* is not in the catalog.
        """
        dataset = self.datasets.get(name)

        if not dataset:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))

        return dataset

    def get_remote_datasets(self) -> list[DatasetConfig]:
        """Return all remote (HTTP) datasets."""
        return [d for d in self.datasets.values() if d.is_remote]

    def get_derived_datasets(self) -> list[DatasetConfig]:
        """Return all derived (Gold) datasets."""
        return [d for d in self.datasets.values() if d.is_derived]

    @model_validator(mode="after")
    def validate_derived_dependencies_exist(self) -> Self:
        """Ensure all ``depends_on`` entries exist and are remote (Silver) datasets."""
        for dataset in self.get_derived_datasets():
            source = dataset.source
            # Type narrowing: derived datasets always have depends_on
            # (validated in SourceConfig)
            assert source.depends_on is not None, (
                f"Derived dataset '{dataset.name}' has no depends_on"
            )
            for dep_name in source.depends_on:
                # Check dependency exists
                if dep_name not in self.datasets:
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on '{dep_name}' "
                        f"which is not defined in the catalog"
                    )

                # Check dependency is remote (has Silver output)
                dep = self.datasets[dep_name]
                if not dep.is_remote:
                    raise ValueError(
                        f"Dataset '{dataset.name}' depends on '{dep_name}' "
                        f"which is itself derived. Gold datasets can only depend on "
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

    logger.info(f"--- Catalog loaded: found {len(catalog.datasets)} dataset(s) ---")

    for _name, _dataset in catalog.datasets.items():
        logger.info(_name, **_dataset.model_dump(exclude={"name"}))
