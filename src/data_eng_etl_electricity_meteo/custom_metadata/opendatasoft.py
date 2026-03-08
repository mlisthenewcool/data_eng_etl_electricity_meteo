"""Remote metadata fetcher for OpenDataSoft export endpoints.

OpenDataSoft ``/exports/parquet`` endpoints generate files on-the-fly and return no HTTP
caching headers (no ETag, Last-Modified, Content-Length).
This module fetches ``data_processed`` (with ``modified`` fallback) from the **catalog
metadata API** and maps it to ``last_modified`` for smart-skip change detection.
"""

import re
from datetime import datetime
from urllib.parse import urlparse

import httpx

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.utils.remote_metadata import RemoteFileMetadata

logger = get_logger("ods_metadata")

# Matches OpenDataSoft v2.1 export URLs and captures the dataset id.
_ODS_EXPORT_RE = re.compile(r"/api/explore/v2\.1/catalog/datasets/(?P<dataset_id>[^/]+)/exports/")


def fetch_ods_metadata(url: str, *, timeout: int = 30) -> RemoteFileMetadata:
    """Fetch ``data_processed`` from the OpenDataSoft catalog API.

    Derives the catalog endpoint from the export *url*, calls it, and returns a
    ``RemoteFileMetadata`` with ``last_modified`` set to ``data_processed`` (preferred)
    or ``modified`` (fallback).

    Parameters
    ----------
    url
        OpenDataSoft export URL
        (e.g. ``https://odre.opendatasoft.com/…/exports/parquet``).
    timeout
        HTTP request timeout in seconds.

    Returns
    -------
    RemoteFileMetadata
        Metadata with ``last_modified`` populated, or empty metadata if neither
        ``data_processed`` nor ``modified`` is available.

    Raises
    ------
    httpx.HTTPStatusError
        If the catalog API returns an error status.
    httpx.TimeoutException
        If the request times out.
    ValueError
        If the URL does not match the OpenDataSoft export pattern.
    """
    parsed = urlparse(url)
    match = _ODS_EXPORT_RE.search(parsed.path)
    if not match:
        raise ValueError(f"URL does not match OpenDataSoft export pattern: {url}")

    dataset_id = match.group("dataset_id")
    catalog_url = (
        f"{parsed.scheme}://{parsed.hostname}/api/explore/v2.1/catalog/datasets/{dataset_id}"
    )

    logger.debug("Fetching catalog metadata", url=catalog_url)

    with httpx.Client(timeout=timeout, http2=True) as client:
        response = client.get(catalog_url)
        response.raise_for_status()

    metas = response.json().get("metas", {}).get("default", {})

    # data_processed tracks the last data update; modified tracks any
    # dataset change (metadata edits included) — less precise but usable.
    data_processed = metas.get("data_processed")
    date_raw: str | None = data_processed or metas.get("modified")
    source = "data_processed" if data_processed else "modified"

    if not date_raw:
        logger.warning(
            "Catalog has no data_processed or modified field",
            dataset_id=dataset_id,
        )
        return RemoteFileMetadata()

    last_modified = datetime.fromisoformat(date_raw)

    logger.debug(
        "Catalog metadata retrieved",
        dataset_id=dataset_id,
        source=source,
        last_modified=last_modified,
    )

    return RemoteFileMetadata(last_modified=last_modified)
