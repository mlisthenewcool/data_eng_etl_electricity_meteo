"""File operations for medallion architecture (symlinks, rotation, rollback, cleanup).

``RemoteFileManager`` handles bronze symlinks, bronze cleanup, and silver
rotation/rollback. Gold datasets live in Postgres (via dbt), not on disk.
"""

import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from data_eng_etl_electricity_meteo.core.enums import MedallionLayer
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver

logger = get_logger("file_manager")


# --------------------------------------------------------------------------------------
# Shared rotation / rollback helpers
# --------------------------------------------------------------------------------------


def _rotate(
    current_path: Path,
    *,
    backup_path: Path,
    layer: MedallionLayer,
) -> None:
    """Copy ``current`` → ``backup``. No-op if current doesn't exist.

    Parameters
    ----------
    current_path
        Path to the current file.
    backup_path
        Path to the backup file.
    layer
        Medallion layer (e.g. ``MedallionLayer.SILVER``).

    Raises
    ------
    OSError
        If the copy fails (permission error, disk full, etc.).
    """
    if current_path.exists():
        previous_mtime = datetime.fromtimestamp(current_path.stat().st_mtime, tz=UTC).strftime(
            "%Y-%m-%dT%H"
        )
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(current_path, backup_path)
        logger.debug("Rotated files", layer=layer, previous_mtime=previous_mtime)
    else:
        logger.warning(
            "Skipped rotation: no current file (expected on first run)",
            layer=layer,
        )


def _rollback(
    current_path: Path,
    *,
    backup_path: Path,
    layer: MedallionLayer,
) -> bool:
    """Restore ``backup`` → ``current``.

    Parameters
    ----------
    current_path
        Path to the current file.
    backup_path
        Path to the backup file.
    layer
        Medallion layer (e.g. ``MedallionLayer.SILVER``).

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
        )
        return False

    shutil.copy2(backup_path, current_path)
    logger.debug("Rolled back to backup version", layer=layer)
    return True


# --------------------------------------------------------------------------------------
# Remote datasets (landing → bronze → silver)
# --------------------------------------------------------------------------------------


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
        target_version
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
                target_version=target_version,
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
        retention_days
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
            file_mtime = datetime.fromtimestamp(timestamp=version_path.stat().st_mtime, tz=UTC)

            if file_mtime < cutoff_time:
                version_path.unlink()
                deleted.append(version_path)
                logger.debug(
                    "Deleted old bronze version",
                    version=version_path.stem,
                    age_days=(now - file_mtime).days,
                )

        if deleted:
            logger.info(
                "Bronze cleanup completed",
                deleted_count=len(deleted),
                retention_days=retention_days,
            )

        return deleted

    def rotate_silver(self) -> None:
        """Copy ``current.parquet`` → ``backup.parquet``.

        Call **before** writing new current. No-op if current doesn't exist.
        """
        _rotate(
            self.resolver.silver_current_path,
            backup_path=self.resolver.silver_backup_path,
            layer=MedallionLayer.SILVER,
        )

    def rollback_silver(self) -> bool:
        """Restore ``backup.parquet`` → ``current.parquet``.

        Returns
        -------
        bool
            ``True`` if rollback succeeded, ``False`` if no backup exists.
        """
        return _rollback(
            self.resolver.silver_current_path,
            backup_path=self.resolver.silver_backup_path,
            layer=MedallionLayer.SILVER,
        )
