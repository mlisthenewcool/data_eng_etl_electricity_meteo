import sys
from datetime import datetime

from data_eng_etl_electricity_meteo.core.exceptions import DatasetNotFoundError, InvalidCatalogError
from data_eng_etl_electricity_meteo.core.logger import logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.data_catalog import DataCatalog

if __name__ == "__main__":
    # ==================================================================================
    # /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ==================================================================================
    _DATASET_NAME = "ign_contours_iris"

    _start_time = datetime.now()

    # ============================================================
    # 1) Load catalog and dataset configuration
    # ============================================================
    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.info(
        "Data catalog loaded",
        remote_datasets=[dataset.name for dataset in _catalog.get_remote_datasets()],
        derivated_datasets=[dataset.name for dataset in _catalog.get_derived_datasets()],
    )

    try:
        _dataset_config = _catalog.get_dataset(_DATASET_NAME)
    except DatasetNotFoundError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.info(
        f"Dataset *{_DATASET_NAME}* config loaded",
        **_dataset_config.model_dump(mode="json", exclude={"name"}),
    )
