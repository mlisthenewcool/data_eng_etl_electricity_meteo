"""Download utilities.

This module provides robust utilities for:
- Downloading large files over HTTP/2 with streaming
- Progress bars with tqdm and SHA256 integrity checks

The download functions are designed for data pipelines where reliability is critical.
Airflow handles retries at the task level, so no retry logic is included here.
"""

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
from tqdm import tqdm

from data_eng_etl_electricity_meteo.core.logger import logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher


@dataclass(frozen=True)
class HttpDownloadResult:
    """Result from HTTP download operation.

    Attributes
    ----------
    path:
        Path to downloaded file (can be an archive or a final file)
    file_hash:
        Hash of downloaded content (uses sha256 by default)
    size_mib:
        File size in mebibyte
    """

    path: Path
    file_hash: str
    size_mib: float

    def to_dict(self) -> dict[str, Path | str | float]:
        """Convert to dict for serialization."""
        return {
            "path": self.path,
            "file_hash": self.file_hash,
            "size_mib": self.size_mib,
        }


def extract_filename_from_response(response: httpx.Response, url: str) -> str | None:
    """Extract original filename from HTTP response or URL.

    Priority order:

    1. Content-Disposition header (most reliable - server specifies filename)
    2. URL path basename (fallback - extract from URL path)

    Parameters
    ----------
    response:
        HTTP response object from httpx
    url:
        Original request URL

    Returns
    -------
    str | None
        Extracted filename (sanitized for filesystem use) if found, None otherwise.
    """
    # Try Content-Disposition header first
    content_disp = response.headers.get("content-disposition", "")
    if content_disp:
        # Parse Content-Disposition header (handles various formats)
        # Examples: "attachment; filename=data.csv"
        #           "attachment; filename*=UTF-8''data%20file.csv"

        # Try standard filename parameter
        regex = r'filename=["\']?([^"\';\n]+)["\']?'
        # regex_simple = r'filename="?([^";\n]+)"?'
        match = re.search(regex, content_disp)
        if match:
            filename = match.group(1).strip()
            # Remove any path separators for security
            filename = Path(filename).name
            if filename and filename != ".":
                logger.info(
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
            logger.info("Extracted filename from URL path.", filename=filename, url=url)
            return filename

    logger.warning("Could not extract filename.", url=url)
    return None


def download_to_file(url: str, dest_dir: Path, default_name: str) -> HttpDownloadResult:
    """Download a file from URL with streaming, progress bar, and SHA256.

    Performs memory-efficient download by streaming chunks to disk.
    Automatically creates parent directories if needed.
    Uses HTTP/2 when available for better performance.

    The filename is extracted from the Content-Disposition header or URL path,
    preserving the original server-provided filename.

    Parameters
    ----------
    url:
        URL of the file to download.
    dest_dir:
        Directory where the file will be saved (filename extracted from response).
    default_name:
        Default name to fallback if any filename could be extracted from server.

    Returns
    -------
    HttpDownloadResult
        The object containing with path, hash, size_mib, and original_filename.

    Raises
    ------
    httpx.HTTPStatusError:
        If server returns error status (4xx/5xx).
    httpx.TimeoutException:
        If request times out.
    """
    logger.info("Starting download", url=url, dest_dir=dest_dir)

    # TODO, documenter & ajouter arguments write/pool
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

            # Extract original filename from response headers or URL
            original_filename = extract_filename_from_response(response, url)
            if original_filename is None:
                logger.warning(
                    "Could not extract original filename, fallback to default.",
                    default_name=default_name,
                )
                original_filename = default_name
            dest_path = dest_dir / original_filename

            if dest_path.exists():
                logger.warning("File already exists. Overwriting.", url=url, dest_path=dest_path)

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            total_bytes = 0
            total_size = int(response.headers.get("content-length", 0))

            hasher = FileHasher()

            progress_bar = tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {original_filename} (total_size={total_size})",
                # file=TqdmToLogger(logger.info)
                # if settings.is_running_on_airflow else sys.stderr,
                file=sys.stderr,
                leave=False,  # disappears when complete
            )

            try:
                with open(dest_path, mode="wb") as f:
                    for chunk in response.iter_bytes(chunk_size=settings.download_chunk_size):
                        f.write(chunk)
                        hasher.update(chunk)
                        chunk_len = len(chunk)
                        total_bytes += chunk_len
                        progress_bar.update(chunk_len)
            finally:
                progress_bar.close()

            file_hash = hasher.hexdigest

            size_mib = total_bytes / (1024 * 1024)

            logger.info(
                "Download completed",
                path=dest_path,
                original_filename=original_filename,
                size_mib=size_mib,
                file_hash=file_hash,
            )

            return HttpDownloadResult(path=dest_path, file_hash=file_hash, size_mib=size_mib)
