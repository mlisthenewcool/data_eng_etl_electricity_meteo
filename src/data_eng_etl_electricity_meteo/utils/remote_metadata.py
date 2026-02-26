"""Remote file change detection via HTTP HEAD metadata."""

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Self

import httpx

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("remote_metadata")


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

    def compare_with(self, other: Self) -> ChangeDetectionResult:
        """Detect changes against a previous state.

        Priority: ETag > Last-Modified > Content-Length.
        Fails safe (reports change) when metadata is missing.

        Parameters
        ----------
        other:
            Previous metadata to compare against.

        Returns
        -------
        ChangeDetectionResult
            ``has_changed=True`` if a change is detected or metadata is
            insufficient to confirm identity; ``False`` only when at least
            one field matches conclusively.
        """
        if not self._has_any_field() or not other._has_any_field():
            return ChangeDetectionResult(True, "Missing metadata on current or previous state")

        for check in (self._check_etag, self._check_last_modified, self._check_content_length):
            if (result := check(other)) is not None:
                return result

        return ChangeDetectionResult(True, "No matching metadata found to confirm identity")

    def _check_etag(self, other: Self) -> ChangeDetectionResult | None:
        """Compare ETags. Returns ``None`` if neither side has an ETag."""
        if not (self.etag and other.etag):
            return None
        if self.etag != other.etag:
            return ChangeDetectionResult(True, f"ETag changed: {other.etag} -> {self.etag}")
        return ChangeDetectionResult(False, "ETag identical")

    def _check_last_modified(self, other: Self) -> ChangeDetectionResult | None:
        """Compare Last-Modified dates. Returns ``None`` if neither side has a date."""
        if not (self.last_modified and other.last_modified):
            return None
        if self.last_modified > other.last_modified:
            return ChangeDetectionResult(True, f"File is newer: {self.last_modified}")
        return ChangeDetectionResult(False, "File date is identical or older")

    def _check_content_length(self, other: Self) -> ChangeDetectionResult | None:
        """Compare Content-Length. Returns ``None`` if neither side has a size."""
        if self.content_length is None or other.content_length is None:
            return None
        if self.content_length != other.content_length:
            return ChangeDetectionResult(
                True, f"Size changed: {other.content_length} -> {self.content_length}"
            )
        return ChangeDetectionResult(False, "Size identical")


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

    Returns
    -------
    RemoteFileMetadata
        Parsed metadata from HTTP HEAD response headers.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status (4xx/5xx).
    httpx.TimeoutException
        If the request times out.
    """
    logger.debug("Checking remote file metadata", url=url)

    with httpx.Client(timeout=timeout, follow_redirects=follow_redirects, http2=True) as client:
        response = client.head(url)
        response.raise_for_status()

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

    logger.debug(
        "Remote file metadata retrieved",
        etag=etag,
        last_modified=last_modified,
        content_length=content_length,
    )

    return metadata
