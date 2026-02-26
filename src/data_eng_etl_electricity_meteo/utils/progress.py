"""Progress reporting primitives: protocols, throttle logic, and tqdm adapters.

Higher-level (Airflow-aware) reporters live in ``pipeline.progress`` and
build on top of the types defined here.
"""

import time
from typing import Protocol

from py7zr.callbacks import ExtractCallback
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOG_INTERVAL_S: float = 10.0
LOG_INTERVAL_PCT: float = 10.0
LOG_MIN_INTERVAL_S: float = 5.0


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


class DownloadProgressReporter(Protocol):
    """Reports byte-count updates during a streaming download.

    Implementations receive the total expected bytes at construction time
    (``0`` when ``Content-Length`` is absent) and individual chunk sizes
    via :meth:`update`.
    """

    def update(self, n: int) -> None:
        """Accumulate *n* downloaded bytes."""

    def close(self) -> None:
        """Release any resources held by the reporter."""


# ---------------------------------------------------------------------------
# Shared throttle logic
# ---------------------------------------------------------------------------


class ThrottledProgressTracker:
    """Track bytes processed and decide when to emit a log line.

    Accumulates byte counts and exposes ``accumulate`` which returns ``True``
    when the elapsed time or percentage-change threshold is crossed.

    Parameters
    ----------
    total_bytes
        Expected total size. ``0`` when unknown (disables %-based triggering).
    """

    def __init__(self, total_bytes: int) -> None:
        self._total = total_bytes
        self._processed: int = 0
        self._last_log_time: float = time.monotonic()
        self._last_log_pct: float = 0.0

    def accumulate(self, n: int) -> bool:
        """Add *n* bytes and return ``True`` if a log line should be emitted."""
        self._processed += n
        now = time.monotonic()
        pct = self._processed / self._total * 100 if self._total else None
        elapsed = now - self._last_log_time

        time_trigger = elapsed >= LOG_INTERVAL_S
        pct_trigger = pct is not None and (pct - self._last_log_pct) >= LOG_INTERVAL_PCT

        if (time_trigger or pct_trigger) and elapsed >= LOG_MIN_INTERVAL_S:
            self._last_log_time = now
            if pct is not None:
                self._last_log_pct = pct
            return True

        return False

    @property
    def processed_mib(self) -> float:
        """Processed bytes converted to MiB (rounded to 2 decimals)."""
        return round(self._processed / 1024**2, 2)

    @property
    def total_mib(self) -> float | None:
        """Total bytes converted to MiB, or ``None`` if unknown."""
        return round(self._total / 1024**2, 2) if self._total else None

    @property
    def pct(self) -> float | None:
        """Current progress percentage, or ``None`` if total is unknown."""
        return round(self._processed / self._total * 100, 2) if self._total else None


# ---------------------------------------------------------------------------
# Extract callback base (py7zr no-op defaults)
# ---------------------------------------------------------------------------


class BaseExtractCallback(ExtractCallback):
    """No-op defaults for all py7zr abstract hooks.

    Subclasses only need to override ``report_update`` to implement
    meaningful progress reporting.
    """

    def report_start(self, processing_file_path: str, processing_bytes: str) -> None:
        """No-op: not needed for progress tracking."""

    def report_end(self, processing_file_path: str, wrote_bytes: str) -> None:
        """No-op: not needed for progress tracking."""

    def report_update(self, decompressed_bytes: str) -> None:
        """No-op: subclasses override this to report progress."""

    def report_start_preparation(self) -> None:
        """No-op: not needed for progress tracking."""

    def report_warning(self, message: str) -> None:
        """No-op: not needed for progress tracking."""

    def report_postprocess(self) -> None:
        """No-op: not needed for progress tracking."""


# ---------------------------------------------------------------------------
# tqdm reporters (local / interactive)
# ---------------------------------------------------------------------------


class TqdmExtractCallback(BaseExtractCallback):
    """Bridge between py7zr extraction and tqdm progress bar."""

    def __init__(self, pbar: tqdm) -> None:
        self.pbar = pbar

    def report_update(self, decompressed_bytes: str) -> None:
        """Update the progress bar with decompressed bytes."""
        self.pbar.update(int(decompressed_bytes))
