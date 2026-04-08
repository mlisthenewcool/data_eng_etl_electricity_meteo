"""Download strategies for remote dataset ingestion.

Plain functions for metadata fetching and file downloading, assembled into
``DownloadStrategy`` dataclasses. Three variants cover all current datasets:

- **Standard** — HTTP HEAD with ETag/304 + single-URL download.
- **ODS** — HEAD with ODS catalog API fallback + single-URL download.
- **DataGouv** — data.gouv.fr dataset API metadata + climatologie download.

Use ``get_strategy(dataset_name)`` to obtain the right strategy for a dataset.
Datasets not in the registry get the standard strategy.
"""

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.custom_downloads.meteo_climatologie import download_climatologie
from data_eng_etl_electricity_meteo.pipeline.progress import (
    AirflowBatchProgress,
    AirflowDownloadProgress,
)
from data_eng_etl_electricity_meteo.pipeline.types import DownloadStrategy
from data_eng_etl_electricity_meteo.utils.download import HttpDownloadInfo, download_to_file
from data_eng_etl_electricity_meteo.utils.remote_metadata import (
    RemoteFileMetadata,
    get_remote_file_metadata,
)

if TYPE_CHECKING:
    from data_eng_etl_electricity_meteo.utils.progress import BatchProgressFactory

logger = get_logger("strategy")


# --------------------------------------------------------------------------------------
# Internal metadata helpers
# --------------------------------------------------------------------------------------


# Matches OpenDataSoft v2.1 export URLs and captures the dataset id.
_ODS_EXPORT_RE = re.compile(r"/api/explore/v2\.1/catalog/datasets/(?P<dataset_id>[^/]+)/exports/")


def _fetch_ods_catalog_metadata(url: str, *, timeout: int = 30) -> RemoteFileMetadata:
    """Fetch ``data_processed`` from the OpenDataSoft catalog API.

    Derives the catalog endpoint from the export *url*, calls it, and returns a
    ``RemoteFileMetadata`` with ``last_modified`` set to ``data_processed`` (preferred)
    or ``modified`` (fallback).

    Parameters
    ----------
    url
        OpenDataSoft export URL.
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

    logger.debug("Fetching ODS catalog metadata", url=catalog_url)

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
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=UTC)

    logger.debug(
        "ODS catalog metadata retrieved",
        dataset_id=dataset_id,
        source=source,
        last_modified=last_modified,
    )

    return RemoteFileMetadata(last_modified=last_modified)


# Matches data.gouv.fr dataset URLs and captures the dataset id or slug.
# Supports both page URLs (/fr/datasets/{id}/) and API URLs
# (/api/1/datasets/{id}/).
_DATAGOUV_DATASET_RE = re.compile(r"data\.gouv\.fr/(?:fr/|api/\d+/)?datasets/(?P<dataset_id>[^/]+)")


def _fetch_datagouv_dataset_metadata(url: str, *, timeout: int = 30) -> RemoteFileMetadata:
    """Fetch dataset-level ``last_update`` from the data.gouv.fr API.

    Derives the dataset identifier from the source *url*, calls the dataset API, and
    returns a ``RemoteFileMetadata`` with ``last_modified`` set to ``last_update``.

    Parameters
    ----------
    url
        data.gouv.fr dataset URL.
    timeout
        HTTP request timeout in seconds.

    Returns
    -------
    RemoteFileMetadata
        Metadata with ``last_modified`` set to ``last_update``, or empty metadata if the
        field is absent.

    Raises
    ------
    httpx.HTTPStatusError
        If the API returns an error status.
    httpx.TimeoutException
        If the request times out.
    ValueError
        If the URL does not match the data.gouv.fr dataset pattern.
    """
    match = _DATAGOUV_DATASET_RE.search(url)
    if not match:
        raise ValueError(f"URL does not match data.gouv.fr dataset pattern: {url}")

    dataset_id = match.group("dataset_id")
    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"

    logger.debug("Fetching data.gouv.fr dataset metadata", url=api_url)

    with httpx.Client(timeout=timeout, http2=True) as client:
        response = client.get(api_url)
        response.raise_for_status()

    last_update_raw: str | None = response.json().get("last_update")

    if not last_update_raw:
        logger.warning(
            "Dataset has no last_update field",
            dataset_id=dataset_id,
        )
        return RemoteFileMetadata()

    last_modified = datetime.fromisoformat(last_update_raw)
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=UTC)

    logger.debug(
        "data.gouv.fr dataset metadata retrieved",
        dataset_id=dataset_id,
        last_modified=last_modified,
    )

    return RemoteFileMetadata(last_modified=last_modified)


# --------------------------------------------------------------------------------------
# Public metadata fetchers (satisfy MetadataFetcher callable)
# --------------------------------------------------------------------------------------


def head_metadata(url: str, previous_etag: str | None) -> RemoteFileMetadata | None:
    """Fetch remote metadata via HTTP HEAD with ETag/304 support.

    Parameters
    ----------
    url
        Remote file URL.
    previous_etag
        ETag from the previous run. Sent as ``If-None-Match`` to enable 304.

    Returns
    -------
    RemoteFileMetadata | None
        Metadata from HEAD response, or ``None`` on HTTP 304.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status (4xx/5xx, excluding 304).
    httpx.TimeoutException
        If the request times out.
    """
    return get_remote_file_metadata(url, if_none_match=previous_etag)


def ods_metadata(url: str, previous_etag: str | None) -> RemoteFileMetadata | None:
    """Fetch metadata via HEAD, fall back to ODS catalog API.

    Parameters
    ----------
    url
        OpenDataSoft export URL.
    previous_etag
        ETag from the previous run.

    Returns
    -------
    RemoteFileMetadata | None
        Metadata from HEAD or ODS catalog, or ``None`` on HTTP 304.

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns an error status.
    httpx.TimeoutException
        If the request times out.
    ValueError
        If the URL does not match the OpenDataSoft export pattern
        (only raised on catalog fallback).
    """
    result = get_remote_file_metadata(url, if_none_match=previous_etag)

    if result is None:
        return None

    if result.has_any_field():
        return result

    # HEAD returned no caching headers — use the catalog API.
    return _fetch_ods_catalog_metadata(url)


def datagouv_metadata(url: str, _previous_etag: str | None) -> RemoteFileMetadata | None:
    """Fetch dataset-level metadata from data.gouv.fr API (soft-fail).

    On any HTTP or parsing error, returns empty metadata instead of raising, so the
    pipeline always proceeds to download.

    Parameters
    ----------
    url
        data.gouv.fr dataset URL.
    _previous_etag
        Ignored (data.gouv.fr API does not support ETags).

    Returns
    -------
    RemoteFileMetadata | None
        Metadata with ``last_modified``, or empty metadata on failure.
        Never returns ``None`` (no 304 support).
    """
    try:
        return _fetch_datagouv_dataset_metadata(url)
    except ValueError as err:
        logger.warning(
            "data.gouv.fr metadata skipped: URL pattern mismatch",
            error=str(err),
            url=url,
        )
        return RemoteFileMetadata()
    except httpx.HTTPError as err:
        logger.warning(
            "data.gouv.fr metadata fetch failed, proceeding",
            error=str(err),
        )
        return RemoteFileMetadata()


# --------------------------------------------------------------------------------------
# Public file downloaders (satisfy FileDownloader callable)
# --------------------------------------------------------------------------------------


def standard_download(
    url: str,
    dest_dir: Path,
    fallback_filename: str,
    timeout_seconds: int,
) -> HttpDownloadInfo:
    """Download via streaming HTTP GET with Airflow-aware progress.

    Parameters
    ----------
    url
        Remote file URL.
    dest_dir
        Landing directory (created if needed).
    fallback_filename
        Filename to use when the server provides none.
    timeout_seconds
        Maximum total download time.

    Returns
    -------
    HttpDownloadInfo
        Downloaded file path, content hash, and size.

    Raises
    ------
    httpx.HTTPError
        On any HTTP failure.
    OSError
        If writing to disk fails.
    """
    return download_to_file(
        url,
        dest_dir=dest_dir,
        fallback_filename=fallback_filename,
        timeout_seconds=timeout_seconds,
        progress=(AirflowDownloadProgress if settings.is_running_on_airflow else None),
    )


def climatologie_download(  # noqa: PLR0913
    _url: str,
    dest_dir: Path,
    _fallback_filename: str,
    _timeout_seconds: int,
    year_start: int | None = None,
    year_end: int | None = None,
) -> HttpDownloadInfo:
    """Download climatologie data (95 departments) with Airflow-aware progress.

    Extra kwargs ``year_start`` / ``year_end`` have defaults so the function satisfies
    ``FileDownloader`` as-is.
    Use ``functools.partial`` to bind overrides from CLI arguments.

    Parameters
    ----------
    _url
        Ignored (climatologie manages its own URLs internally).
    dest_dir
        Landing directory for the merged Parquet file.
    _fallback_filename
        Ignored (climatologie uses a fixed filename).
    _timeout_seconds
        Ignored (climatologie manages its own timeouts).
    year_start
        Start year for the 2-year window. Defaults to ``current_year - 1``.
    year_end
        End year for the 2-year window. Defaults to ``current_year``.

    Returns
    -------
    HttpDownloadInfo
        Merged file path, content hash, and size.

    Raises
    ------
    DownloadError
        If no data could be downloaded from any department.
    OSError
        If the merge of per-department Parquet files fails.
    """
    progress: BatchProgressFactory | None = (
        AirflowBatchProgress if settings.is_running_on_airflow else None
    )
    return download_climatologie(
        dest_dir, progress=progress, year_start=year_start, year_end=year_end
    )


# --------------------------------------------------------------------------------------
# Strategy registry
# --------------------------------------------------------------------------------------


_STANDARD = DownloadStrategy(fetch_metadata=head_metadata, download_file=standard_download)
_ODS = DownloadStrategy(fetch_metadata=ods_metadata, download_file=standard_download)

_STRATEGIES: dict[str, DownloadStrategy] = {
    "odre_eco2mix_tr": _ODS,
    "odre_eco2mix_cons_def": _ODS,
    "meteo_france_climatologie": DownloadStrategy(
        fetch_metadata=datagouv_metadata,
        download_file=climatologie_download,
    ),
}


def get_strategy(dataset_name: str) -> DownloadStrategy:
    """Return the download strategy for *dataset_name*.

    Parameters
    ----------
    dataset_name
        Catalog identifier. Datasets not in the registry get the standard strategy.

    Returns
    -------
    DownloadStrategy
        Strategy instance with metadata fetcher and file downloader.
    """
    return _STRATEGIES.get(dataset_name, _STANDARD)
