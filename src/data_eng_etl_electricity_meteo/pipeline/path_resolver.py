"""Path resolution for the medallion architecture layers.

``RemotePathResolver`` handles landing / bronze / silver paths for remote HTTP datasets.
Gold datasets live in Postgres (via dbt), not on disk.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from data_eng_etl_electricity_meteo.core.settings import settings


@dataclass(frozen=True)
class _BasePathResolver:
    """Common base: dataset name + base directory."""

    dataset_name: str
    base_dir: Path = field(init=False, default_factory=lambda: settings.data_dir_path)

    def __post_init__(self) -> None:
        """Validate that dataset_name is not empty.

        Notes
        -----
        Necessary because resolvers can be instantiated directly
        (e.g. in tests or scripts) without going through the catalog, where an empty
        name would silently produce broken paths (e.g. ``bronze/latest.parquet`` instead
        of ``bronze/{dataset_name}/latest.parquet``).

        Raises
        ------
        ValueError
            If ``dataset_name`` is empty or whitespace-only.
        """
        if not self.dataset_name.strip():
            raise ValueError("dataset_name must be a non-empty string")


@dataclass(frozen=True)
class RemotePathResolver(_BasePathResolver):
    """Path construction for remote datasets: landing → bronze → silver."""

    # -- Landing Layer (Temporary) -----------------------------------------------------

    @property
    def landing_dir(self) -> Path:
        """Temporary storage dir, deleted after Bronze conversion."""
        return self.base_dir / "landing" / self.dataset_name

    # -- Bronze Layer (Versioned History) ----------------------------------------------

    @property
    def _bronze_dir(self) -> Path:
        return self.base_dir / "bronze" / self.dataset_name

    def bronze_path(self, version: str) -> Path:
        """Return ``bronze/{dataset_name}/{version}.parquet``.

        Parameters
        ----------
        version
            Version string (e.g. ``"2026-01-17"``).

        Returns
        -------
        Path
            Versioned bronze file path.
        """
        return self._bronze_dir / f"{version}.parquet"

    @property
    def bronze_latest_path(self) -> Path:
        """Symlink to most recent bronze version."""
        return self._bronze_dir / "latest.parquet"

    def bronze_latest_version(self) -> str | None:
        """Version string from ``latest.parquet`` symlink target.

        Returns
        -------
        str | None
            Version string, or ``None`` if no symlink exists.
        """
        latest_link = self.bronze_latest_path

        if not latest_link.exists() or not latest_link.is_symlink():
            return None

        # Bronze filenames are "{version}.parquet", so .stem recovers the version.
        return latest_link.readlink().stem

    def list_bronze_versions(self) -> list[Path]:
        """All bronze version files sorted newest-first.

        Excludes the ``latest.parquet`` symlink.

        Returns
        -------
        list[Path]
            Version files sorted by name descending, empty if no dir.
        """
        if not self._bronze_dir.exists():
            return []

        # latest.parquet is a symlink and must not appear in version listings
        versions: list[Path] = [
            path
            for path in self._bronze_dir.glob("*.parquet")
            if path.is_file() and not path.is_symlink()
        ]

        return sorted(versions, key=lambda p: p.name, reverse=True)

    # -- Silver Layer (Current + Backup) -----------------------------------------------

    @property
    def _silver_dir(self) -> Path:
        return self.base_dir / "silver" / self.dataset_name

    @property
    def silver_current_path(self) -> Path:
        """Active silver version consumed by downstream processes."""
        return self._silver_dir / "current.parquet"

    @property
    def silver_backup_path(self) -> Path:
        """Previous silver version (N-1) for fast rollback."""
        return self._silver_dir / "backup.parquet"

    @property
    def silver_delta_path(self) -> Path:
        """Delta file containing only new/changed rows for incremental loads."""
        return self._silver_dir / "delta.parquet"


if __name__ == "__main__":
    import sys

    from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
    from data_eng_etl_electricity_meteo.core.exceptions import InvalidCatalogError
    from data_eng_etl_electricity_meteo.core.logger import get_logger

    logger = get_logger("path_resolver")

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(logger.critical)
        sys.exit(-1)

    for _dataset in _catalog.get_remote_datasets():
        _run_version = _dataset.ingestion.frequency.format_datetime_as_version(
            dt=datetime.now(tz=UTC)
        )

        _resolver = RemotePathResolver(dataset_name=_dataset.name)

        logger.debug(
            "Remote dataset paths",
            run_version=_run_version,
            landing_dir=_resolver.landing_dir,
            bronze_path=_resolver.bronze_path(version=_run_version),
            bronze_latest_path=_resolver.bronze_latest_path,
            bronze_latest_version=_resolver.bronze_latest_version(),
            silver_backup_path=_resolver.silver_backup_path,
            silver_current_path=_resolver.silver_current_path,
            silver_delta_path=_resolver.silver_delta_path,
        )
