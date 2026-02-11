"""Data catalog configuration for managing dataset sources and ingestion policies.

This module provides a declarative, YAML-based catalog system for defining:
- Where to fetch data (source URLs, formats, providers)
- When to fetch data (ingestion frequency, mode)
- How data is versioned (frequency-based: daily, hourly, etc.)

The catalog is validated using Pydantic models, ensuring type safety and catching
configuration errors at load time rather than runtime.

Architecture:
    DataCatalog
        └── datasets: dict[str, Dataset]
            ├── name: str (auto-injected from YAML key)
            ├── description: str
            ├── source: Source (provider, URL, format, inner_file)
            └── ingestion: IngestionPolicy (frequency, mode)

Note:
    Path resolution is handled by PathResolver, not Dataset directly.
    See path_resolver.py for medallion architecture path management.
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
    """Supported source file formats for dataset ingestion.

    Attributes:
        SEVEN_Z: 7-Zip compressed archive format
        PARQUET: Apache Parquet columnar storage format
        JSON: JSON data interchange format
    """

    SEVEN_Z = "7z"
    PARQUET = "parquet"
    JSON = "json"

    @property
    def is_archive(self) -> bool:
        """Check if the format is a compressed archive requiring extraction."""
        return self == SourceFormat.SEVEN_Z


class IngestionFrequency(StrEnum):
    """Expected data update frequency from the source provider.

    This enum defines how often the data source is expected to publish
    new data, which influences the Airflow scheduling strategy.

    Each enum member combines two pieces of information:
    - The frequency value (e.g., "hourly", "daily") used in YAML configs
    - The corresponding Airflow schedule expression (e.g., "@hourly", "@daily")

    This design ensures that adding a new frequency automatically includes
    its Airflow schedule, preventing inconsistencies between the two.

    Attributes:
        HOURLY: Data updated every hour (Airflow: @hourly)
        DAILY: Data updated daily (Airflow: @daily)
        WEEKLY: Data updated weekly (Airflow: @weekly)
        MONTHLY: Data updated monthly (Airflow: @monthly)
        YEARLY: Data updated yearly (Airflow: @yearly)
        NEVER: Never automatically update (Airflow: None)

    Implementation Note:
        This enum uses a custom __new__ method to store both the string value
        and the Airflow schedule as instance attributes. This pattern ensures
        strong coupling between frequencies and their schedules, making it
        impossible to forget to define a schedule when adding a new frequency.

    Example:
        >>> freq = IngestionFrequency.DAILY
        >>> print(freq)  # String value
        'daily'
        >>> print(freq.airflow_schedule)  # Airflow schedule
        '@daily'
        >>> print(freq.value)  # Also accessible via .value
        'daily'
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
        """Create enum member with both value and Airflow schedule.

        This custom __new__ method allows each enum member to store two
        pieces of information: the string value (used in configs) and the
        corresponding Airflow schedule expression.

        Args:
            value: Frequency string value (e.g., "hourly", "daily")
            airflow_schedule: Airflow cron preset (e.g., "@hourly") or None

        Returns:
            Enum member with both value and schedule attributes.

        Note:
            This pattern is the recommended way to add extra data to enum
            members in Python. The str.__new__ call ensures StrEnum behavior
            is preserved (enum members behave like strings).
        """
        obj = str.__new__(cls, value)
        obj._value_ = value
        # Store schedule in private attribute (dynamic attribute on enum)
        obj._airflow_schedule = airflow_schedule
        return obj

    @property
    def airflow_schedule(self) -> str | None:
        """Get the Airflow schedule expression for this frequency.

        Returns:
            Airflow cron preset string (e.g., "@daily") or None if frequency
            is 'never'. This value is stored at enum member creation time.

        Example:
            >>> IngestionFrequency.DAILY.airflow_schedule
            '@daily'
            >>> IngestionFrequency.NEVER.airflow_schedule
            None
        """
        # Use getattr to satisfy type checkers (attribute set dynamically in __new__)
        return getattr(self, "_airflow_schedule", None)

    def get_airflow_version_template(self, no_dash: bool = False) -> str:
        """Get Airflow template variable based on ingestion frequency.

        Returns {{ ts_nodash }} for hourly datasets, {{ ds_nodash }} otherwise.
        This is used to pass the correct template to tasks.

        Args:
            no_dash: If True, return nodash version ({{ ds_nodash }} or {{ ts_nodash }}).
                     If False, return version with separators ({{ ds }} or {{ ts }}).
                     Default: False.

        Returns:
            Airflow template string that will be replaced at runtime.
            - Hourly: "{{ ts_nodash }}" (20260121T143022) or "{{ ts }}" (2026-01-21T14:30:22)
            - Others: "{{ ds_nodash }}" (20260121) or "{{ ds }}" (2026-01-21)

        Raises:
            Exception: If not running on Airflow (check settings.is_running_on_airflow)

        Example:
            >>> freq = IngestionFrequency.DAILY
            >>> freq.get_airflow_version_template(no_dash=True)
            '{{ ds_nodash }}'  # For daily/weekly/monthly datasets
            >>> freq = IngestionFrequency.HOURLY
            >>> freq.get_airflow_version_template(no_dash=True)
            '{{ ts_nodash }}'  # For hourly datasets
        """
        if not settings.is_running_on_airflow:
            # raise AirflowContextError(
            #     operation="`get_airflow_version_template`",
            #     expected_context="airflow",
            #     actual_context="not airflow",
            #     suggestion="use `format_datetime_as_version` instead",
            # )
            pass

        if self == IngestionFrequency.HOURLY:
            # TODO: "{{ data_interval_start.strftime('%Y%m%dT%H') }}" ou "{{ ts_nodash[:11] }}"
            return "{{ ts_nodash }}" if no_dash else "{{ ts }}"

        return "{{ ds_nodash }}" if no_dash else "{{ ds }}"

    def format_datetime_as_version(self, dt: datetime, no_dash: bool = False) -> str:
        """Format datetime as version string matching Airflow template format.

        This method generates a version string from a datetime object that matches
        the format Airflow would produce from its template variables.
        Use this in non-Airflow contexts (scripts, notebooks, tests).

        Args:
            dt: Datetime to format as version string
            no_dash: If True, return compact format without separators.
                     If False, return format with dashes/colons.
                     Default: False.

        Returns:
            Version string formatted according to frequency:
            - Hourly (no_dash=True): "20260121T143022" (YYYYMMDDTHHmmss)
            - Hourly (no_dash=False): "2026-01-21-T-14-30-22"
            - Daily/Weekly/Monthly/Yearly/Never (no_dash=True): "20260121" (YYYYMMDD)
            - Daily/Weekly/Monthly/Yearly/Never (no_dash=False): "2026-01-21" (YYYY-MM-DD)

        Raises:
            Exception: If running on Airflow (should use get_airflow_version_template instead)

        Example:
            >>> from datetime import datetime
            >>> freq = IngestionFrequency.DAILY
            >>> freq.format_datetime_as_version(datetime(2026, 1, 21), no_dash=True)
            '20260121'
            >>> freq = IngestionFrequency.HOURLY
            >>> freq.format_datetime_as_version(datetime(2026, 1, 21, 14, 30, 22), no_dash=True)
            '20260121T143022'
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
    """Configuration for dataset sources - either remote (HTTP) or derived (Gold layer).

    This model handles both remote and derived sources with mutual exclusivity validation.
    A source is either:
    - Remote: fetched via HTTP (requires provider, url, format)
    - Derived: built from Silver datasets (requires depends_on)

    Attributes:
        provider: Data provider name (e.g., "IGN", "ODRE") - for remote sources only
        url: HTTP(S) URL to download the source file - for remote sources only
        format: Source file format (7z, parquet, JSON) - for remote sources only
        inner_file: For archive formats only - target file to extract - for remote sources only
        depends_on: List of Silver dataset names - for derived sources only

    Validation Rules:
        - Must have EITHER (url + provider + format) OR depends_on, not both
        - Archive formats (7z) MUST specify inner_file
        - Non-archive formats MUST NOT specify inner_file
        - depends_on must not be empty if specified

    Example:
        Remote source (archive):

        >>> data = {
        ...     "provider": "IGN",
        ...     "url": "https://data.ign.fr/contours.7z",
        ...     "format": "7z",
        ...     "inner_file": "iris.gpkg"
        ... }
        >>> source = SourceConfig.model_validate(data)

        Remote source (direct):

        >>> data = {
        ...     "provider": "ODRE",
        ...     "url": "https://data.odre.fr/installations.parquet",
        ...     "format": "parquet"
        ... }
        >>> source = SourceConfig.model_validate(data)

        Derived source:

        >>> data = {
        ...     "depends_on": ["odre_installations", "ign_contours_iris"]
        ... }
        >>> source = SourceConfig.model_validate(data)
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
        """Validate that fields are consistent with source type (remote vs derived).

        Returns:
            Self instance if validation passes.

        Raises:
            ValueError: If fields don't match remote or derived patterns,
                       or if both patterns are present.
        """
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
        """Check if this is a remote source (HTTP download)."""
        return self.url is not None

    @property
    def is_derived(self) -> bool:
        """Check if this is a derived source (Gold layer)."""
        return self.depends_on is not None

    @property
    def url_as_str(self) -> str:
        """Convert HttpUrl to string for compatibility with download functions.

        Returns:
            String representation of the URL.

        Raises:
            ValueError: If called on a derived source (url is None).

        Example:
            >>> source.url_as_str # noqa
            'https://data.ign.fr/file.7z'
        """
        if self.url is None:
            raise ValueError("url_as_str called on derived source (url is None)")
        return str(self.url)


class IngestionMode(StrEnum):
    """Data ingestion mode defining how updates are processed.

    Attributes:
        SNAPSHOT: Full dataset replacement on each ingestion
        INCREMENTAL: Only new/changed records are ingested (requires incremental_key)

    Note:
        Incremental mode is not yet fully implemented. The incremental_key
        field will be added in a future version.
    """

    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"


class IngestionPolicy(StrictModel):
    """Data ingestion configuration defining frequency and mode.

    This model controls when and how data should be fetched from the source.
    Versioning is handled dynamically based on frequency (daily, hourly, etc.)
    via IngestionFrequency.format_datetime_as_version() or Airflow templates.

    Attributes:
        frequency: Expected update frequency from the source (e.g., daily, hourly).
                   Determines version format: daily → "YYYYMMDD", hourly → "YYYYMMDDTHHMMSS"
        mode: IngestionPolicy mode (snapshot or incremental)

    Example:
        >>> ingestion = IngestionPolicy(
        ...     frequency=IngestionFrequency.MONTHLY,
        ...     mode=IngestionMode.SNAPSHOT
        ... )
        >>>
        >>> # Version generation (outside Airflow)
        >>> from datetime import datetime
        >>> version = ingestion.frequency.format_datetime_as_version(
        ...     datetime.now(), no_dash=True
        ... )
        >>> print(version)  # "20260121" for monthly/daily

    Note:
        Future versions will add incremental_key field for incremental mode,
        with validation requiring it when mode=INCREMENTAL.
    """

    frequency: IngestionFrequency
    mode: IngestionMode
    # incremental_key: str  # TODO: Add validation -> required if mode=INCREMENTAL


class DatasetConfig(StrictModel):
    """Configuration for a dataset - either remote (HTTP) or derived (Gold layer).

    This model handles both remote and derived datasets with automatic type detection
    based on source configuration. The dataset type is determined by the source fields:
    - Remote: source has url/provider/format (and optional ingestion)
    - Derived: source has depends_on (no ingestion)

    Attributes:
        name: Dataset identifier (e.g., "ign_contours_iris", "installations_meteo")
        description: Human-readable dataset description
        source: Source configuration (remote or derived)
        ingestion: Ingestion policy (frequency, mode) - required for remote, forbidden for derived

    Example:
        Remote dataset:

        >>> dataset = DatasetConfig(
        ...     name="ign_contours_iris",
        ...     description="IGN administrative boundaries",
        ...     source=SourceConfig(
        ...         provider="IGN",
        ...         url="https://data.geopf.fr/...",
        ...         format="7z",
        ...         inner_file="iris.gpkg"
        ...     ),
        ...     ingestion=IngestionPolicy(
        ...         frequency="daily",
        ...         mode="snapshot"
        ...     ),
        ... )

        Derived dataset:

        >>> dataset = DatasetConfig(
        ...     name="installations_meteo",
        ...     description="Installations with nearest weather station",
        ...     source=SourceConfig(
        ...         depends_on=["odre_installations", "ign_contours_iris", "meteo_france_stations"]
        ...     ),
        ... )
    """

    name: str
    description: str
    source: SourceConfig
    ingestion: IngestionPolicy | None = None

    @model_validator(mode="after")
    def validate_ingestion_consistency(self) -> Self:
        """Validate that ingestion field is consistent with source type.

        Remote datasets MUST have ingestion policy.
        Derived datasets MUST NOT have ingestion policy.

        Returns:
            Self instance if validation passes.

        Raises:
            ValueError: If ingestion is missing for remote or present for derived.
        """
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
        """Check if this is a remote dataset (HTTP download)."""
        return self.source.is_remote

    @property
    def is_derived(self) -> bool:
        """Check if this is a derived dataset (Gold layer)."""
        return self.source.is_derived


class DataCatalog(StrictModel):
    """Root catalog model containing all dataset configurations.

    This is the top-level model loaded from data_catalog.yaml. It provides
    access to all registered datasets with validation and type safety.

    Attributes:
        datasets: Dictionary mapping dataset names to Dataset configurations

    Example:
        Load and access catalog:

        >>> catalog = DataCatalog.load(Path("data/catalog.yaml"))
        >>> print(catalog.datasets.keys())
        dict_keys(['ign_contours_iris', 'odre_installations'])
        >>> dataset = catalog.get_dataset("ign_contours_iris")
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
        """Load and validate the data catalog from YAML file.

        Reads the YAML catalog file, validates it against the schema using
        Pydantic, and returns a fully validated DataCatalog instance.

        Args:
            path: Path to the YAML catalog file.

        Returns:
            Validated DataCatalog instance with all datasets loaded.

        Raises:
            InvalidCatalogError: If the catalog file doesn't exist, contains
                                 invalid YAML, or fails Pydantic validation.
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
        """Retrieve a dataset configuration by name.

        Args:
            name: Dataset identifier (e.g., "ign_contours_iris").

        Returns:
            DatasetConfig (either DatasetRemote or DatasetDerived) for the requested dataset.

        Raises:
            DatasetNotFoundError: If the dataset doesn't exist in the catalog.
                The error includes a list of available datasets.

        Example:
            >>> dataset = catalog.get_dataset("ign_contours_iris") # noqa
            >>> dataset.description
            'IGN administrative boundaries and IRIS zones'

            >>> catalog.get_dataset("unknown_dataset") # noqa
            DatasetNotFoundError: Dataset 'unknown_dataset' not found.
            Available: ['ign_contours_iris', 'odre_installations']
        """
        dataset = self.datasets.get(name)

        if not dataset:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))

        return dataset

    def get_remote_datasets(self) -> list[DatasetConfig]:
        """Return all datasets fetched from remote sources.

        Returns:
            List of datasets with remote source configuration (url/provider/format).
        """
        return [d for d in self.datasets.values() if d.is_remote]

    def get_derived_datasets(self) -> list[DatasetConfig]:
        """Return all datasets derived from Silver sources.

        Returns:
            List of datasets with derived source configuration (depends_on) - Gold layer.
        """
        return [d for d in self.datasets.values() if d.is_derived]

    @model_validator(mode="after")
    def validate_derived_dependencies_exist(self) -> Self:
        """Ensure all derived dataset dependencies reference existing datasets.

        Validates that:
        1. All dependencies exist in the catalog
        2. All dependencies are remote datasets (have Silver output)
        3. No circular dependencies (Gold cannot depend on Gold)

        Returns:
            Self instance if validation passes.

        Raises:
            ValueError: If dependency validation fails.
        """
        for dataset in self.get_derived_datasets():
            source = dataset.source
            # Type narrowing: derived datasets always have depends_on (validated in SourceConfig)
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

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError:
        # logger.error(str(e), extra=e.validation_errors)
        sys.exit(1)

    # logger.info(f"Catalog loaded: found {len(_catalog.datasets)} dataset(s)")

    for _name, _dataset in _catalog.datasets.items():
        # logger.info(f"dataset: {_name}", extra=_dataset.model_dump(exclude={"name"}))
        print(_name)
