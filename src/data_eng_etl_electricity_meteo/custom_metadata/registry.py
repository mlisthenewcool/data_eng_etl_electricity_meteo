"""Registry of custom metadata strategies.

Maps dataset names to alternative metadata fetchers for smart-skip change detection.
Used as a fallback when HTTP HEAD returns no caching headers (OpenDataSoft) or as the
primary metadata source for custom downloads (Météo France climatologie).
Imported by both the Airflow DAG factory and standalone CLI scripts.
"""

from functools import partial

from data_eng_etl_electricity_meteo.custom_downloads.meteo_climatologie import (
    DATAGOUV_METEO_FRANCE_CLIMATOLOGIE_HOR_DATASET_ID,
)
from data_eng_etl_electricity_meteo.custom_metadata.datagouv import (
    fetch_datagouv_dataset_metadata,
)
from data_eng_etl_electricity_meteo.custom_metadata.opendatasoft import (
    fetch_ods_metadata,
)
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import CustomMetadataFunc

CUSTOM_METADATA: dict[str, CustomMetadataFunc] = {
    "odre_eco2mix_tr": fetch_ods_metadata,
    "odre_eco2mix_cons_def": fetch_ods_metadata,
    "meteo_france_climatologie": partial(
        fetch_datagouv_dataset_metadata,
        dataset_id=DATAGOUV_METEO_FRANCE_CLIMATOLOGIE_HOR_DATASET_ID,
    ),
}
