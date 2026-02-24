"""File operations for medallion architecture (symlinks, rotation, rollback, cleanup).

Two manager types match the two dataset types:

- ``RemoteFileManager``  → bronze symlinks, bronze cleanup, silver rotation/rollback
- ``DerivedFileManager`` → gold rotation/rollback
"""

import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
    DerivedPathResolver,
    RemotePathResolver,
)

logger = get_logger("file_manager")


# ---------------------------------------------------------------------------
# Shared rotation / rollback helpers
# ---------------------------------------------------------------------------


def _rotate(
    dataset_name: str,
    current_path: Path,
    backup_path: Path,
    layer: str,
) -> None:
    """Copy ``current`` → ``backup``. No-op if current doesn't exist.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (for log context).
    current_path:
        Path to the current file.
    backup_path:
        Path to the backup file.
    layer:
        Layer name for log messages (e.g. ``"silver"``, ``"gold"``).

    Raises
    ------
    OSError
        If the copy fails (permission error, disk full, etc.).
    """
    if current_path.exists():
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(current_path, backup_path)
        logger.debug(
            "Rotated files",
            layer=layer,
            dataset_name=dataset_name,
            current=current_path,
            backup=backup_path,
        )
    else:
        logger.warning(
            "Skipped rotation: no current file (expected on first run)",
            layer=layer,
            dataset_name=dataset_name,
        )


def _rollback(
    dataset_name: str,
    current_path: Path,
    backup_path: Path,
    layer: str,
) -> bool:
    """Restore ``backup`` → ``current``.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (for log context).
    current_path:
        Path to the current file.
    backup_path:
        Path to the backup file.
    layer:
        Layer name for log messages (e.g. ``"silver"``, ``"gold"``).

    Returns
    -------
    bool
        ``True`` if rollback succeeded, ``False`` if no backup exists.

    Raises
    ------
    OSError
        If the copy fails (permission error, disk full, etc.).
    """
    if not backup_path.exists():
        logger.warning(
            "Cannot rollback: no backup exists",
            layer=layer,
            dataset_name=dataset_name,
        )
        return False

    shutil.copy2(backup_path, current_path)
    logger.debug(
        "Rolled back to backup version",
        layer=layer,
        dataset_name=dataset_name,
        backup=backup_path,
        current=current_path,
    )
    return True


# ---------------------------------------------------------------------------
# Remote datasets (landing → bronze → silver)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RemoteFileManager:
    """File operations for remote datasets.

    Handles bronze symlinks, bronze cleanup, silver rotation/rollback.
    """

    resolver: RemotePathResolver

    def update_bronze_latest_link(self, target_version: str) -> None:
        """Atomically update the ``latest.parquet`` symlink.

        Call **after** writing a new bronze file.

        Parameters
        ----------
        target_version:
            Version string of the bronze file to point to.

        Raises
        ------
        FileNotFoundError
            If the target bronze file does not exist.
        """
        target_file = self.resolver.bronze_path(version=target_version)
        latest_link = self.resolver.bronze_latest_path

        if not target_file.exists():
            raise FileNotFoundError(
                f"Cannot create symlink: target bronze file doesn't exist: {target_file}"
            )

        latest_link.parent.mkdir(parents=True, exist_ok=True)

        # Atomic symlink update:
        # 1. Create temporary symlink with unique name
        # 2. Atomically rename it to replace old symlink
        temp_link = latest_link.parent / f".latest.tmp.{os.getpid()}"

        try:
            # Relative path is more portable than absolute
            relative_target = target_file.name
            temp_link.symlink_to(relative_target)
            temp_link.replace(latest_link)

            logger.debug(
                "Updated bronze latest symlink",
                dataset_name=self.resolver.dataset_name,
                target_version=target_version,
                symlink=latest_link,
                target=relative_target,
            )
        except OSError:
            if temp_link.exists():
                temp_link.unlink()
            raise

    def cleanup_old_bronze_versions(
        self, retention_days: int = settings.bronze_retention_days
    ) -> list[Path]:
        """Remove bronze versions older than the retention period.

        Parameters
        ----------
        retention_days:
            Number of days to retain versions (default from settings).

        Returns
        -------
        list[Path]
            Paths of deleted version files.
        """
        now = datetime.now(tz=UTC)
        cutoff_time = now - timedelta(days=retention_days)
        deleted = []

        for version_path in self.resolver.list_bronze_versions():
            file_mtime = datetime.fromtimestamp(version_path.stat().st_mtime, tz=UTC)

            if file_mtime < cutoff_time:
                version_path.unlink()
                deleted.append(version_path)
                logger.debug(
                    "Deleted old bronze version",
                    dataset_name=self.resolver.dataset_name,
                    version=version_path.stem,
                    age_days=(now - file_mtime).days,
                )

        if deleted:
            logger.info(
                "Bronze cleanup completed",
                dataset_name=self.resolver.dataset_name,
                deleted_count=len(deleted),
                retention_days=retention_days,
            )

        return deleted

    def rotate_silver(self) -> None:
        """Copy ``current.parquet`` → ``backup.parquet``.

        Call **before** writing new current. No-op if current doesn't exist.
        """
        _rotate(
            self.resolver.dataset_name,
            self.resolver.silver_current_path,
            self.resolver.silver_backup_path,
            "silver",
        )

    def rollback_silver(self) -> bool:
        """Restore ``backup.parquet`` → ``current.parquet``.

        Returns
        -------
        bool
            ``True`` if rollback succeeded, ``False`` if no backup exists.
        """
        return _rollback(
            self.resolver.dataset_name,
            self.resolver.silver_current_path,
            self.resolver.silver_backup_path,
            "silver",
        )


# ---------------------------------------------------------------------------
# Derived datasets (gold)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DerivedFileManager:
    """File operations for derived datasets: gold rotation/rollback."""

    resolver: DerivedPathResolver

    def rotate_gold(self) -> None:
        """Copy ``current.parquet`` → ``backup.parquet``.

        Call **before** writing new current. No-op if current doesn't exist.
        """
        _rotate(
            self.resolver.dataset_name,
            self.resolver.gold_current_path,
            self.resolver.gold_backup_path,
            "gold",
        )

    def rollback_gold(self) -> bool:
        """Restore ``backup.parquet`` → ``current.parquet``.

        Returns
        -------
        bool
            ``True`` if rollback succeeded, ``False`` if no backup exists.
        """
        return _rollback(
            self.resolver.dataset_name,
            self.resolver.gold_current_path,
            self.resolver.gold_backup_path,
            "gold",
        )
