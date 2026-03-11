"""Remote file change detection via HTTP HEAD metadata."""

import http
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Self

import httpx

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("remote_metadata")


@dataclass(frozen=True, slots=True)
class ChangeDetectionResult:
    """Change detection verdict (boolean-evaluable)."""

    has_changed: bool
    reason: str

    def __bool__(self) -> bool:
        """Allow ``if result:`` usage."""
        return self.has_changed


@dataclass(frozen=True, slots=True)
class RemoteFileMetadata:
    """ETag, Last-Modified, and Content-Length from an HTTP HEAD."""

    etag: str | None = None
    last_modified: datetime | None = None
    content_length: int | None = None

    def has_any_field(self) -> bool:
        """Check if at least one metadata field is populated."""
        return any(v is not None for v in [self.etag, self.last_modified, self.content_length])

    def compare_with(self, other: Self) -> ChangeDetectionResult:
        """Detect changes against a previous state.

        Priority: ETag > Last-Modified > Content-Length.
        Fails safe (reports change) when metadata is missing.

        Parameters
        ----------
        other
            Previous metadata to compare against.

        Returns
        -------
        ChangeDetectionResult
            ``has_changed=True`` if a change is detected or metadata is insufficient to
            confirm identity; ``False`` only when at least one field matches
            conclusively.
        """
        if not self.has_any_field() or not other.has_any_field():
            return ChangeDetectionResult(
                has_changed=True,
                reason="Missing metadata on current or previous state",
            )

        for check in (self._check_etag, self._check_last_modified, self._check_content_length):
            if (result := check(other)) is not None:
                return result

        return ChangeDetectionResult(
            has_changed=True,
            reason="No matching metadata found to confirm identity",
        )

    def _check_etag(self, other: Self) -> ChangeDetectionResult | None:
        """Compare ETags. Returns ``None`` if either side lacks an ETag."""
        if not (self.etag and other.etag):
            return None
        if self.etag != other.etag:
            return ChangeDetectionResult(
                has_changed=True,
                reason=f"ETag changed: {other.etag} -> {self.etag}",
            )
        return ChangeDetectionResult(has_changed=False, reason="ETag identical")

    def _check_last_modified(self, other: Self) -> ChangeDetectionResult | None:
        """Compare Last-Modified dates. Returns ``None`` if either side lacks a date."""
        if not (self.last_modified and other.last_modified):
            return None
        if self.last_modified == other.last_modified:
            return ChangeDetectionResult(has_changed=False, reason="File date is identical")
        if self.last_modified > other.last_modified:
            return ChangeDetectionResult(
                has_changed=True,
                reason=f"File is newer: {self.last_modified}",
            )
        return ChangeDetectionResult(
            has_changed=True,
            reason=f"File date went backward (rollback?): {self.last_modified}",
        )

    def _check_content_length(self, other: Self) -> ChangeDetectionResult | None:
        """Compare Content-Length. Returns ``None`` if neither side has a size."""
        if self.content_length is None or other.content_length is None:
            return None
        if self.content_length != other.content_length:
            return ChangeDetectionResult(
                has_changed=True,
                reason=f"Size changed: {other.content_length} -> {self.content_length}",
            )
        return ChangeDetectionResult(has_changed=False, reason="Size identical")


def get_remote_file_metadata(
    url: str,
    *,
    timeout: int = 30,
    follow_redirects: bool = True,
    if_none_match: str | None = None,
) -> RemoteFileMetadata | None:
    """Fetch ETag, Last-Modified, and Content-Length via HTTP HEAD.

    When *if_none_match* is provided, sends an ``If-None-Match`` header.
    If the server responds with **304 Not Modified**, returns ``None``
    (the remote file has not changed since the ETag was issued).

    Parameters
    ----------
    url
        Remote file URL.
    timeout
        Request timeout in seconds.
    follow_redirects
        Whether to follow HTTP redirects.
    if_none_match
        ETag value from a previous run.
        Sent as ``If-None-Match`` header to enable HTTP 304 short-circuit.

    Returns
    -------
    RemoteFileMetadata | None
        Parsed metadata from HTTP HEAD response headers, or ``None`` if the server
        returned 304 Not Modified.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status (4xx/5xx, excluding 304).
    httpx.TimeoutException
        If the request times out.
    """
    logger.debug("Checking remote file metadata", url=url)

    request_headers: dict[str, str] = {}
    if if_none_match is not None:
        request_headers["If-None-Match"] = f'"{if_none_match}"'

    with httpx.Client(timeout=timeout, follow_redirects=follow_redirects, http2=True) as client:
        response = client.head(url, headers=request_headers)

        if response.status_code == http.HTTPStatus.NOT_MODIFIED:
            logger.debug("HTTP 304 Not Modified", url=url)
            return None

        response.raise_for_status()

    headers = response.headers

    # ETags may be quoted (RFC 7232 §2.3) or weak-prefixed (W/"…")
    raw_etag = headers.get("etag", "")
    if raw_etag.startswith('W/"'):
        etag = raw_etag[3:-1] or None
    else:
        etag = raw_etag.strip('"') or None

    # Parse Last-Modified (RFC 7231 HTTP-date format)
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
