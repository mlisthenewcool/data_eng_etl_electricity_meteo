"""Remote file change detection via HTTP HEAD metadata."""

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Self

import httpx

from data_eng_etl_electricity_meteo.core.logger import logger

__all__: list[str] = [
    "ChangeDetectionResult",
    "RemoteFileMetadata",
    "get_remote_file_metadata",
    "has_remote_file_changed",
]


@dataclass(frozen=True)
class ChangeDetectionResult:
    """Change detection verdict (boolean-evaluable)."""

    has_changed: bool
    reason: str

    def __bool__(self) -> bool:
        """Allow ``if result:`` usage."""
        return self.has_changed


@dataclass(frozen=True)
class RemoteFileMetadata:
    """ETag, Last-Modified, and Content-Length from an HTTP HEAD."""

    etag: str | None = None
    last_modified: datetime | None = None
    content_length: int | None = None

    def _has_any_field(self) -> bool:
        """Check if at least one metadata field is populated."""
        return any(v is not None for v in [self.etag, self.last_modified, self.content_length])

    def compare_with(self, other: Self) -> ChangeDetectionResult:  # noqa: PLR0911
        """Detect changes against a previous state.

        Priority: ETag > Last-Modified > Content-Length.
        Fails safe (reports change) when metadata is missing.

        Parameters
        ----------
        other:
            Previous metadata to compare against.
        """
        # 1. Uncertainty Case (Fail-safe)
        if not self._has_any_field() or not other._has_any_field():
            return ChangeDetectionResult(True, "Missing metadata on current or previous state")

        # 2. ETag Check (Highest priority)
        if self.etag and other.etag:
            if self.etag != other.etag:
                return ChangeDetectionResult(True, f"ETag changed: {other.etag} -> {self.etag}")
            return ChangeDetectionResult(False, "ETag identical")

        # 3. Date Check
        if self.last_modified and other.last_modified:
            if self.last_modified > other.last_modified:
                return ChangeDetectionResult(True, f"File is newer: {self.last_modified}")
            return ChangeDetectionResult(False, "File date is identical or older")

        # 4. Size Check
        if self.content_length is not None and other.content_length is not None:
            if self.content_length != other.content_length:
                return ChangeDetectionResult(
                    True, f"Size changed: {other.content_length} -> {self.content_length}"
                )
            return ChangeDetectionResult(False, "Size identical")

        return ChangeDetectionResult(True, "No matching metadata found to confirm identity")


def get_remote_file_metadata(
    url: str,
    timeout: int = 30,
    follow_redirects: bool = True,
) -> RemoteFileMetadata:
    """Fetch ETag, Last-Modified, and Content-Length via HTTP HEAD.

    Parameters
    ----------
    url:
        Remote file URL.
    timeout:
        Request timeout in seconds.
    follow_redirects:
        Whether to follow HTTP redirects.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status (4xx/5xx).
    httpx.TimeoutException
        If the request times out.
    """
    logger.info("Checking remote file metadata", url=url)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=follow_redirects, http2=True) as client:
            response = client.head(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP error checking remote file: {e.response.status_code}",
            url=url,
            status=e.response.status_code,
        )
        raise
    except httpx.TimeoutException:
        logger.error("Timeout checking remote file", url=url, timeout=timeout)
        raise
    except httpx.HTTPError as e:
        logger.error("Network error checking remote file", url=url, error=str(e))
        raise

    headers = response.headers

    # Parse ETag (remove quotes if present)
    etag = headers.get("etag", "").strip('"') or None

    # Parse Last-Modified (RFC 2822 format)
    last_modified = None
    if "last-modified" in headers:
        try:
            last_modified = parsedate_to_datetime(headers["last-modified"])
        except (TypeError, ValueError) as e:
            logger.warning(
                "Invalid Last-Modified header",
                header=headers["last-modified"],
                error=str(e),
            )

    # Parse Content-Length
    content_length = None
    if "content-length" in headers:
        try:
            content_length = int(headers["content-length"])
        except ValueError as e:
            logger.warning(
                "Invalid Content-Length header",
                header=headers["content-length"],
                error=str(e),
            )

    metadata = RemoteFileMetadata(
        etag=etag, last_modified=last_modified, content_length=content_length
    )

    logger.info(
        "Remote file metadata retrieved",
        etag=etag,
        last_modified=last_modified,
        content_length=content_length,
    )

    return metadata


def has_remote_file_changed(
    current: RemoteFileMetadata, previous: RemoteFileMetadata
) -> ChangeDetectionResult:
    """Compare two metadata snapshots to detect changes.

    Parameters
    ----------
    current:
        Newly fetched metadata.
    previous:
        Metadata from a previous execution.
    """
    # TODO: ajouter méthode If-None-Match
    # headers = {}
    # if previous_etag:
    #     headers["If-None-Match"] = f'"{previous_etag}"'  # Format standard avec quotes
    # if previous_modified:
    #     fmt = "%a, %d %b %Y %H:%M:%S GMT"
    #     headers["If-Modified-Since"] = previous_modified.strftime(fmt)
    #
    # try:
    #     with httpx.Client(timeout=timeout, follow_redirects=True) as client:
    #         # On tente un GET léger ou un HEAD
    #         response = client.head(url, headers=headers)
    #
    #         # CAS 304 : Le serveur confirme que rien n'a changé
    #         if response.status_code == 304:
    #             logger.info("Server returned 304: File unchanged", extra={"url": url})
    #             return RemoteFileMetadata(
    #                 url=url, etag=previous_etag, available=True
    #             ), False
    #
    #         response.raise_for_status()
    #
    # except httpx.HTTPError:
    #     raise

    return current.compare_with(previous)
