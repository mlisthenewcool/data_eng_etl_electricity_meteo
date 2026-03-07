"""Registry of custom metadata strategies.

Datasets whose HTTP HEAD returns no caching headers register a fallback metadata fetcher
here. Imported by both the Airflow DAG factory and standalone CLI scripts.
"""

from data_eng_etl_electricity_meteo.custom_metadata.opendatasoft import (
    fetch_ods_metadata,
)
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import CustomMetadataFunc

CUSTOM_METADATA: dict[str, CustomMetadataFunc] = {
    "odre_eco2mix_tr": fetch_ods_metadata,
    "odre_eco2mix_cons_def": fetch_ods_metadata,
}
