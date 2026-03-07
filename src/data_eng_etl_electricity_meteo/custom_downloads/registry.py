"""Registry of custom download strategies.

Datasets that need a custom download (not single-URL) register here.
Imported by both the Airflow DAG factory and standalone CLI scripts.
"""

from data_eng_etl_electricity_meteo.custom_downloads.meteo_climatologie import (
    download_climatologie,
)
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import CustomDownloadFunc

CUSTOM_DOWNLOADS: dict[str, CustomDownloadFunc] = {
    "meteo_france_climatologie": download_climatologie,
}
