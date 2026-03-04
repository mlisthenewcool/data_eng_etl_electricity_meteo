"""HTTP download utilities with streaming, progress bar and hash calculation.

Notes
-----
Airflow handles retries at the task level, so no retry logic is included here.
"""

import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
from tqdm import tqdm

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher
from data_eng_etl_electricity_meteo.utils.progress import DownloadProgressReporter

logger = get_logger("download")


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HttpDownloadInfo:
    """Downloaded file information (path, hash, size)."""

    path: Path
    file_hash: str
    size_mib: float


# ---------------------------------------------------------------------------
# Filename extraction
# ---------------------------------------------------------------------------


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
    # Try Content-Disposition header first
    content_disp = response.headers.get("content-disposition", "")
    if content_disp:
        # Parse Content-Disposition header (handles various formats)
        # Examples: "attachment; filename=data.csv"
        #           "attachment; filename*=UTF-8''data%20file.csv"

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

    # Fallback: extract from URL path
    url_path = urlparse(url).path
    if url_path:
        # decode URL encoding, unquote handles %20 and other special chars
        filename = Path(unquote(url_path)).name

        # check if it's an actual file and not a folder
        if filename and filename != "." and "." in filename:
            logger.debug("Extracted filename from URL path", filename=filename, url=url)
            return filename

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


# TODO: accept timeout and chunk_size as function parameters instead of
#  reading directly from settings (download_timeout_total,
#  download_timeout_connect, download_timeout_sock_read, download_chunk_size)
def download_to_file(
    url: str,
    dest_dir: Path,
    fallback_filename: str,
    progress: Callable[[int], DownloadProgressReporter] | None = None,
) -> HttpDownloadInfo:
    """Stream a file from *url* to *dest_dir* with progress and SHA256.

    Parameters
    ----------
    url
        URL of the file to download.
    dest_dir
        Destination directory (created if needed).
    fallback_filename
        Fallback filename if none could be extracted from the response.
    progress
        Factory called with ``total_bytes`` (``0`` if unknown) that returns a
        :class:`DownloadProgressReporter`.
        Pass ``None`` (default) to use the built-in tqdm progress bar.

    Returns
    -------
    HttpDownloadInfo
        Downloaded file path, SHA-256 hash, and size in MiB.

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
    logger.info("Starting download", url=url, dest_dir=dest_dir)

    # TODO: document and expose write/pool timeout parameters
    timeout = httpx.Timeout(
        timeout=settings.download_timeout_total,
        connect=settings.download_timeout_connect,
        read=settings.download_timeout_sock_read,
        write=None,
        pool=None,
    )

    with httpx.Client(http2=True, timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()

            filename = _extract_filename(response, url)
            if filename is None:
                logger.warning(
                    "Could not extract filename, fallback to default",
                    fallback_filename=fallback_filename,
                )
                filename = fallback_filename
            dest_path = dest_dir / filename

            if dest_path.exists():
                logger.warning("File already exists, overwriting", url=url, dest_path=dest_path)

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded_bytes = 0
            content_length = int(response.headers.get("content-length", 0))

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
                    for chunk in response.iter_bytes(chunk_size=settings.download_chunk_size):
                        f.write(chunk)
                        hasher.update(chunk)
                        chunk_len = len(chunk)
                        downloaded_bytes += chunk_len
                        reporter.update(chunk_len)
            finally:
                reporter.close()

            file_hash = hasher.hexdigest
            size_mib = round(downloaded_bytes / (1024 * 1024), 2)

            logger.info("Download completed", filename=filename, size_mib=size_mib)

            return HttpDownloadInfo(path=dest_path, file_hash=file_hash, size_mib=size_mib)
