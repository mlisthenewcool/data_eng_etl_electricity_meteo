"""Path resolution for the medallion architecture layers.

Two resolver types match the two dataset types:

- ``RemotePathResolver``  → landing / bronze / silver (remote HTTP datasets)
- ``DerivedPathResolver`` → gold (derived datasets built from silver sources)
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from data_eng_etl_electricity_meteo.core.settings import settings


@dataclass(frozen=True)
class _BasePathResolver:
    """Common base: dataset name + base directory."""

    dataset_name: str
    _base_dir: Path = settings.data_dir_path

    def __post_init__(self) -> None:
        """Validate that dataset_name is not empty.

        Raises
        ------
        ValueError
            If ``dataset_name`` is empty or whitespace-only.
        """
        if not self.dataset_name or not self.dataset_name.strip():
            raise ValueError("dataset_name must be a non-empty string")


@dataclass(frozen=True)
class RemotePathResolver(_BasePathResolver):
    """Path construction for remote datasets: landing → bronze → silver."""

    # =========================================================================
    # Landing Layer (Temporary)
    # =========================================================================

    @property
    def landing_dir(self) -> Path:
        """Temporary storage dir, deleted after Bronze conversion."""
        return self._base_dir / "landing" / self.dataset_name

    # =========================================================================
    # Bronze Layer (Versioned History)
    # =========================================================================

    @property
    def _bronze_dir(self) -> Path:
        return self._base_dir / "bronze" / self.dataset_name

    def bronze_path(self, version: str) -> Path:
        """Return ``bronze/{dataset_name}/{version}.parquet``.

        Parameters
        ----------
        version:
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

        # Resolve symlink to get target filename
        # .stem: extract version from filename (remove .parquet)
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

        # Exclude symlink and only include regular files
        # TODO: speed vs security (alternative: p.stem != "latest")
        versions: list[Path] = [
            path
            for path in self._bronze_dir.glob("*.parquet")
            if path.is_file() and not path.is_symlink()
        ]

        return sorted(versions, key=lambda p: p.name, reverse=True)

    # =========================================================================
    # Silver Layer (Current + Backup)
    # =========================================================================

    @property
    def _silver_dir(self) -> Path:
        return self._base_dir / "silver" / self.dataset_name

    @property
    def silver_current_path(self) -> Path:
        """Active silver version consumed by downstream processes."""
        return self._silver_dir / "current.parquet"

    @property
    def silver_backup_path(self) -> Path:
        """Previous silver version (N-1) for fast rollback."""
        return self._silver_dir / "backup.parquet"


@dataclass(frozen=True)
class DerivedPathResolver(_BasePathResolver):
    """Path construction for derived datasets: gold only."""

    @property
    def _gold_dir(self) -> Path:
        return self._base_dir / "gold" / self.dataset_name

    @property
    def gold_current_path(self) -> Path:
        """Active gold version (analytical table from joined Silver sources)."""
        return self._gold_dir / "current.parquet"

    @property
    def gold_backup_path(self) -> Path:
        """Previous gold version (N-1) for fast rollback."""
        return self._gold_dir / "backup.parquet"


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

    # Remote datasets: landing → bronze → silver
    for _dataset in _catalog.get_remote_datasets():
        _run_version = _dataset.ingestion.frequency.format_datetime_as_version(datetime.now())

        _resolver = RemotePathResolver(dataset_name=_dataset.name)

        logger.info(
            "Remote dataset paths",
            dataset_name=_dataset.name,
            run_version=_run_version,
            landing_dir=_resolver.landing_dir,
            bronze_path=_resolver.bronze_path(_run_version),
            bronze_latest_path=_resolver.bronze_latest_path,
            bronze_latest_version=_resolver.bronze_latest_version(),
            silver_backup_path=_resolver.silver_backup_path,
            silver_current_path=_resolver.silver_current_path,
        )

    # Derived datasets: gold only
    for _dataset in _catalog.get_derived_datasets():
        _resolver = DerivedPathResolver(dataset_name=_dataset.name)

        logger.info(
            "Derived dataset paths",
            dataset_name=_dataset.name,
            gold_current_path=_resolver.gold_current_path,
            gold_backup_path=_resolver.gold_backup_path,
        )
