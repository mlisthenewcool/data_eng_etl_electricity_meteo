"""Airflow-aware progress reporters for download and extraction stages.

Used by ``RemoteIngestionPipeline`` to inject structured log progress into
Airflow task logs instead of tqdm bars.

Pass these classes (not instances) as the ``progress`` factory argument of
:func:`~data_eng_etl_electricity_meteo.utils.download.download_to_file` and
:func:`~data_eng_etl_electricity_meteo.utils.extraction.extract_7z`:

    download_to_file(..., progress=AirflowDownloadProgress)
    extract_7z(..., progress=AirflowExtractProgress)
"""

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.utils.progress import (
    BaseExtractCallback,
    ThrottledProgressTracker,
)

logger = get_logger("progress")


# ---------------------------------------------------------------------------
# Airflow reporters (structlog-based)
# ---------------------------------------------------------------------------


class AirflowDownloadProgress:
    """Logs download progress via structlog every X seconds or Y%.

    *total_bytes* is ``0`` when the ``Content-Length`` header is absent;
    percentage-based triggering is then disabled.
    """

    def __init__(self, total_bytes: int) -> None:
        self._tracker = ThrottledProgressTracker(total_bytes)

    def update(self, n: int) -> None:
        """Accumulate *n* bytes and log if a threshold is crossed."""
        if self._tracker.accumulate(n):
            pct = self._tracker.pct
            logger.info(
                "Download progress",
                downloaded_mib=self._tracker.processed_mib,
                **(
                    {"total_mib": self._tracker.total_mib, "progress_pct": pct}
                    if pct is not None
                    else {"total_mib": "unknown"}
                ),
            )

    def close(self) -> None:
        """No-op: required by ``DownloadProgressReporter`` protocol."""


class AirflowExtractProgress(BaseExtractCallback):
    """Logs extraction progress via structlog every X seconds or Y%.

    Unlike downloads, the total uncompressed size is always known from the
    7z archive metadata — percentage is always available.
    """

    def __init__(self, total_bytes: int) -> None:
        self._tracker = ThrottledProgressTracker(total_bytes)

    def report_update(self, decompressed_bytes: str) -> None:
        """Accumulate decompressed bytes and log if a threshold is crossed."""
        if self._tracker.accumulate(int(decompressed_bytes)):
            logger.info(
                "Extraction progress",
                decompressed_mib=self._tracker.processed_mib,
                total_mib=self._tracker.total_mib,
                progress_pct=self._tracker.pct,
            )
