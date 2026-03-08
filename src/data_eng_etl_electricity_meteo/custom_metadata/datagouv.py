"""Remote metadata fetcher for data.gouv.fr datasets.

Fetches ``last_update`` from the data.gouv.fr dataset API to enable smart-skip for
custom downloads that bypass the standard HTTP HEAD check. Use ``functools.partial`` to
bind ``dataset_id`` and match the ``CustomMetadataFunc`` signature.
"""

from datetime import datetime

import httpx

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.utils.remote_metadata import RemoteFileMetadata

logger = get_logger("datagouv_metadata")


def fetch_datagouv_dataset_metadata(
    url: str, *, dataset_id: str, timeout: int = 30
) -> RemoteFileMetadata:
    """Fetch dataset-level ``last_update`` from the data.gouv.fr API.

    Parameters
    ----------
    url
        Dataset source URL (unused, required by ``CustomMetadataFunc`` signature).
    dataset_id
        data.gouv.fr dataset identifier.
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
    """
    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"

    logger.debug("Fetching dataset metadata", url=api_url)

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

    logger.debug(
        "Dataset metadata retrieved",
        dataset_id=dataset_id,
        last_modified=last_modified,
    )

    return RemoteFileMetadata(last_modified=last_modified)
