"""HTTP download utilities with streaming, progress bar and hash calculation.

Notes
-----
Airflow handles retries at the task level, so no retry logic is included here.
"""

import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import httpx
from tqdm import tqdm

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher
from data_eng_etl_electricity_meteo.utils.progress import DownloadProgressReporter

logger = get_logger("download")

# Optimal streaming chunk size (bytes). Benchmarked sweet spot is 256 KB–1 MB;
# below 64 KB syscall overhead dominates, above 1 MB RAM grows with no speed gain.
_CHUNK_SIZE = 512 * 1024

# Standard timeouts (seconds) for TCP connect and socket read.
_CONNECT_TIMEOUT = 10
_READ_TIMEOUT = 30


# --------------------------------------------------------------------------------------
# Types
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class HttpDownloadInfo:
    """Downloaded file information (path, hash, size)."""

    path: Path
    file_hash: str
    size_mib: float


# --------------------------------------------------------------------------------------
# URL and filename helpers
# --------------------------------------------------------------------------------------


_GENERIC_PATH_SEGMENTS = frozenset(
    {
        "exports",
        "export",
        "download",
        "parquet",
        "json",
        "csv",
    }
)


def _short_url(url: str) -> str:
    """Shorten a URL to ``hostname/…/meaningful_segment`` for log readability.

    Skips generic path segments (``exports``, ``parquet``, …) to surface the most
    informative part of the URL.
    """
    parsed = urlparse(url)
    parts = [p for p in PurePosixPath(unquote(parsed.path)).parts if p != "/"]
    for part in reversed(parts):
        if part.lower() not in _GENERIC_PATH_SEGMENTS:
            return f"{parsed.hostname}/…/{part}"
    name = PurePosixPath(unquote(parsed.path)).name or parsed.path
    return f"{parsed.hostname}/…/{name}"


def _extract_filename(response: httpx.Response, url: str) -> str | None:
    """Extract filename from Content-Disposition header or URL path.

    Parameters
    ----------
    response
        HTTP response with headers to inspect.
    url
        Original request URL (fallback source for filename).

    Returns
    -------
    str | None
        Sanitized filename, or ``None`` if extraction failed.
    """
    # Content-Disposition is more reliable than URL path for server-generated names
    content_disp = response.headers.get("content-disposition", "")
    if content_disp:
        # Parse Content-Disposition (filename= only; filename*= RFC 5987 not supported)

        regex = r'filename=["\']?([^"\';\n]+)["\']?'
        match = re.search(regex, content_disp)
        if match:
            filename = match.group(1).strip()
            # Remove any path separators for security
            filename = Path(filename).name
            if filename and filename != ".":
                logger.debug(
                    "Extracted filename from Content-Disposition",
                    filename=filename,
                    header=content_disp,
                )
                return filename

    # URL path is less reliable but works for static file hosting
    url_path = urlparse(url).path
    if url_path:
        filename = Path(unquote(url_path)).name

        # Reject directory-like paths (/api/v2/) that have no file extension
        if filename and filename != "." and "." in filename:
            logger.debug("Extracted filename from URL path", filename=filename, url=url)
            return filename

    return None


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def download_to_file(
    url: str,
    *,
    dest_dir: Path,
    fallback_filename: str,
    timeout_seconds: int,
    progress: Callable[[int], DownloadProgressReporter] | None = None,
) -> HttpDownloadInfo:
    """Stream a file from *url* to *dest_dir* with progress and integrity hash.

    Parameters
    ----------
    url
        URL of the file to download.
    dest_dir
        Destination directory (created if needed).
    fallback_filename
        Fallback filename if none could be extracted from the response.
    timeout_seconds
        Maximum total time for the entire download. Connect and read timeouts use
        module-level defaults (``_CONNECT_TIMEOUT``, ``_READ_TIMEOUT``).
    progress
        Factory called with ``total_bytes`` (``0`` if unknown) that returns a
        `DownloadProgressReporter`.
        Pass ``None`` (default) to use the built-in tqdm progress bar.

    Returns
    -------
    HttpDownloadInfo
        Downloaded file path, content hash, and size in MiB.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status (4xx/5xx).
    httpx.TimeoutException
        If the request times out (connect or read timeout).
    httpx.ConnectError
        If the host cannot be reached.
    OSError
        If the destination file cannot be written.
    """
    logger.info("Starting download", url=_short_url(url))
    logger.debug("Download URL", url=url, dest_dir=dest_dir)

    timeout = httpx.Timeout(
        timeout=timeout_seconds,
        connect=_CONNECT_TIMEOUT,
        read=_READ_TIMEOUT,
        write=None,
        pool=None,
    )

    with httpx.Client(http2=True, timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()

            # -- Resolve destination filename and path ---------------------------------

            filename = _extract_filename(response, url)
            if filename is None:
                logger.warning(
                    "Could not extract filename, fallback to default",
                    fallback_filename=fallback_filename,
                )
                filename = fallback_filename
            dest_path = dest_dir / filename

            if dest_path.exists():
                logger.warning("File already exists, overwriting", url=_short_url(url))

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded_bytes = 0
            try:
                content_length = int(response.headers.get("content-length", 0))
            except ValueError:
                logger.warning(
                    "Invalid Content-Length header",
                    header=response.headers.get("content-length"),
                )
                content_length = 0

            # -- Initialize progress reporter and stream -------------------------------

            hasher = FileHasher()

            reporter: DownloadProgressReporter = (
                progress(content_length)
                if progress is not None
                else tqdm(
                    total=content_length,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Downloading {filename}",
                    file=sys.stderr,
                    leave=False,
                    mininterval=1.0,
                )
            )

            try:
                with dest_path.open("wb") as f:
                    for chunk in response.iter_bytes(chunk_size=_CHUNK_SIZE):
                        f.write(chunk)
                        hasher.update(chunk)
                        chunk_len = len(chunk)
                        downloaded_bytes += chunk_len
                        reporter.update(chunk_len)
            finally:
                reporter.close()

            # -- Compute final metadata and return -------------------------------------

            size_mib = round(downloaded_bytes / (1024 * 1024), 2)

            logger.info("Download completed", filename=filename, file_size_mib=size_mib)

            return HttpDownloadInfo(dest_path, file_hash=hasher.hexdigest, size_mib=size_mib)
