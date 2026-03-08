"""Local JSON-based persistence for pipeline run snapshots.

Provides the single source of truth for smart-skip decisions, shared by both Airflow
tasks and CLI runs via the ``data/_state/`` directory.

Each dataset gets one file: ``data/_state/{dataset_name}.json``, overwritten atomically
after each successful silver stage via temp file + ``os.rename()``
(POSIX atomic on same filesystem).
"""

import contextlib
import os
import tempfile
from pathlib import Path

import orjson

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.types import PipelineRunSnapshot

logger = get_logger("state")


def _state_path(dataset_name: str) -> Path:
    """Return ``data/_state/{dataset_name}.json``."""
    return settings.data_state_dir_path / f"{dataset_name}.json"


def load_local_snapshot(dataset_name: str) -> PipelineRunSnapshot | None:
    """Load the previous run snapshot from the local JSON state file.

    Parameters
    ----------
    dataset_name
        Dataset identifier.

    Returns
    -------
    PipelineRunSnapshot | None
        Validated snapshot, or ``None`` if the file is missing or corrupted.
    """
    path = _state_path(dataset_name)

    if not path.exists():
        return None

    try:
        raw = orjson.loads(path.read_bytes())
    except (orjson.JSONDecodeError, OSError) as exc:
        logger.warning("State file unreadable, treating as first run", error=str(exc))
        return None

    return PipelineRunSnapshot.from_metadata_dict(raw)


def save_local_snapshot(dataset_name: str, snapshot: PipelineRunSnapshot) -> None:
    """Persist a run snapshot to the local JSON state file (atomic write).

    Parameters
    ----------
    dataset_name
        Dataset identifier.
    snapshot
        Completed pipeline run snapshot to persist.
    """
    path = _state_path(dataset_name)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(
                orjson.dumps(
                    snapshot.model_dump(mode="json", exclude_none=True),
                )
            )
        os.rename(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise

    logger.debug("State saved")
