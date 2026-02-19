"""Airflow-aware progress reporters for download and extraction stages.

Used by ``RemoteDatasetPipeline`` to inject structured log progress into
Airflow task logs instead of tqdm bars.

Pass these classes (not instances) as the ``progress`` factory argument of
:func:`~data_eng_etl_electricity_meteo.utils.download.download_to_file` and
:func:`~data_eng_etl_electricity_meteo.utils.extraction.extract_7z`:

    download_to_file(..., progress=AirflowDownloadProgress)
    extract_7z(..., progress=AirflowExtractProgress)
"""

import time

from py7zr.callbacks import ExtractCallback

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("progress")

_LOG_INTERVAL_S: float = 10.0
_LOG_INTERVAL_PCT: float = 10.0
_LOG_MIN_INTERVAL_S: float = 5.0


class AirflowDownloadProgress:
    """Logs download progress via structlog every X seconds or Y%.

    Attributes
    ----------
    _total:
        Expected total bytes (``0`` when ``Content-Length`` is absent).
    """

    def __init__(self, total_bytes: int) -> None:
        self._total = total_bytes
        self._downloaded: int = 0
        self._last_log_time: float = time.monotonic()
        self._last_log_pct: float = 0.0

    def update(self, n: int) -> None:
        """Accumulate *n* bytes and log if a threshold is crossed."""
        self._downloaded += n
        _now = time.monotonic()
        _pct = self._downloaded / self._total * 100 if self._total else None
        _elapsed = _now - self._last_log_time
        _time_trigger = _elapsed >= _LOG_INTERVAL_S
        _pct_trigger = _pct is not None and (_pct - self._last_log_pct) >= _LOG_INTERVAL_PCT

        if (_time_trigger or _pct_trigger) and _elapsed >= _LOG_MIN_INTERVAL_S:
            _total_mib = round(self._total / 1024**2, 2) if self._total else None
            logger.info(
                "Download progress",
                downloaded_mib=round(self._downloaded / 1024**2, 2),
                **(
                    {"total_mib": _total_mib, "progress_pct": round(_pct, 2)}
                    if _pct is not None
                    else {"total_mib": "unknown"}
                ),
            )
            self._last_log_time = _now
            if _pct is not None:
                self._last_log_pct = _pct

    def close(self) -> None:
        """No-op: required by ``DownloadProgressReporter`` protocol."""


class AirflowExtractProgress(ExtractCallback):
    """Logs extraction progress via structlog every X seconds or Y%.

    Attributes
    ----------
    _total:
        Uncompressed size in bytes of the target file.
    """

    def __init__(self, total_bytes: int) -> None:
        self._total = total_bytes
        self._decompressed = 0
        self._last_log_time = time.monotonic()
        self._last_log_pct = 0.0

    def report_update(self, decompressed_bytes: str) -> None:
        """Accumulate decompressed bytes and log if a threshold is crossed."""
        self._decompressed += int(decompressed_bytes)
        _now = time.monotonic()
        _pct = self._decompressed / self._total * 100 if self._total else None
        _elapsed = _now - self._last_log_time
        _time_trigger = _elapsed >= _LOG_INTERVAL_S
        _pct_trigger = _pct is not None and (_pct - self._last_log_pct) >= _LOG_INTERVAL_PCT

        if (_time_trigger or _pct_trigger) and _elapsed >= _LOG_MIN_INTERVAL_S:
            logger.info(
                "Extraction progress",
                decompressed_mib=round(self._decompressed / 1024**2, 2),
                **({"progress_pct": round(_pct, 2)} if _pct is not None else {}),
            )
            self._last_log_time = _now
            if _pct is not None:
                self._last_log_pct = _pct

    def report_start(self, processing_file_path: str, processing_bytes: str) -> None:
        """No-op: required by ``ExtractCallback`` protocol."""

    def report_end(self, processing_file_path: str, wrote_bytes: str) -> None:
        """No-op: required by ``ExtractCallback`` protocol."""

    def report_start_preparation(self) -> None:
        """No-op: required by ``ExtractCallback`` protocol."""

    def report_warning(self, message: str) -> None:
        """No-op: required by ``ExtractCallback`` protocol."""

    def report_postprocess(self) -> None:
        """No-op: required by ``ExtractCallback`` protocol."""
