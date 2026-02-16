"""HTTP download utilities with streaming, progress bar and hash calculation.

Notes
-----
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

__all__: list[str] = ["HttpDownloadResult", "download_to_file"]


@dataclass(frozen=True)
class HttpDownloadResult:
    """Downloaded file metadata (path, hash, size)."""

    path: Path
    file_hash: str
    size_mib: float

    def to_dict(self) -> dict[str, str | float]:
        """Serialize to a JSON-compatible dict (for Airflow XCom)."""
        return {
            "path": str(self.path),
            "file_hash": self.file_hash,
            "size_mib": self.size_mib,
        }


def _extract_filename(response: httpx.Response, url: str) -> str | None:
    """Extract filename from Content-Disposition header or URL path.

    Parameters
    ----------
    response:
        HTTP response with headers to inspect.
    url:
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


def download_to_file(url: str, dest_dir: Path, default_filename: str) -> HttpDownloadResult:
    """Stream a file from *url* to *dest_dir* with progress and SHA256.

    Parameters
    ----------
    url:
        URL of the file to download.
    dest_dir:
        Destination directory (created if needed).
    default_filename:
        Fallback filename if none could be extracted from the response.

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

            filename = _extract_filename(response, url)
            if filename is None:
                logger.warning(
                    "Could not extract filename, fallback to default.",
                    default_filename=default_filename,
                )
                filename = default_filename
            dest_path = dest_dir / filename

            if dest_path.exists():
                logger.warning("File already exists. Overwriting.", url=url, dest_path=dest_path)

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded_bytes = 0
            content_length = int(response.headers.get("content-length", 0))

            hasher = FileHasher()

            progress_bar = tqdm(
                total=content_length,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {filename} (content_length={content_length})",
                # file=TqdmToLogger(logger.info)
                # if settings.is_running_on_airflow else sys.stderr,
                file=sys.stderr,
                leave=False,  # disappears when complete
            )

            try:
                with dest_path.open("wb") as f:
                    for chunk in response.iter_bytes(chunk_size=settings.download_chunk_size):
                        f.write(chunk)
                        hasher.update(chunk)
                        chunk_len = len(chunk)
                        downloaded_bytes += chunk_len
                        progress_bar.update(chunk_len)
            finally:
                progress_bar.close()

            file_hash = hasher.hexdigest

            size_mib = downloaded_bytes / (1024 * 1024)

            logger.info(
                "Download completed",
                path=dest_path,
                filename=filename,
                size_mib=size_mib,
                file_hash=file_hash,
            )

            return HttpDownloadResult(path=dest_path, file_hash=file_hash, size_mib=size_mib)
