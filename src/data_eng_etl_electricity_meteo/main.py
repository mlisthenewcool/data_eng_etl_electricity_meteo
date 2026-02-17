"""CLI entrypoint for local pipeline execution."""

import sys
from datetime import datetime

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DatasetNotFoundError,
    ExtractStageError,
    IngestStageError,
    InvalidCatalogError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.remote_dataset_manager import RemoteDatasetPipeline

if __name__ == "__main__":
    # ==================================================================================
    # /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ==================================================================================
    _DATASET_NAME = "ign_contours_iris"

    _start_datetime = datetime.now()

    # ============================================================
    # 0) Load catalog and dataset configuration
    # ============================================================
    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.info("----- (0) Load catalog and dataset configuration -----")
    logger.info(
        "Data catalog loaded",
        remote_datasets=[dataset.name for dataset in _catalog.get_remote_datasets()],
        derived_datasets=[dataset.name for dataset in _catalog.get_derived_datasets()],
    )

    try:
        _dataset_config = _catalog.get_dataset(_DATASET_NAME)
    except DatasetNotFoundError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.info(
        f"Dataset *{_DATASET_NAME}* config loaded",
        dataset_type=type(_dataset_config).__name__,
        **_dataset_config.model_dump(mode="json", exclude={"name"}),
    )

    # ============================================================
    # 1) Prepare version, RemotePathResolver and RemoteDatasetPipeline
    # ============================================================
    logger.info("----- (1) Prepare version and required classes -----")

    assert isinstance(_dataset_config, RemoteDatasetConfig), "todo: créer get_remote_dataset"
    _version = _dataset_config.ingestion.frequency.format_datetime_as_version(_start_datetime)
    _path_resolver = RemotePathResolver(dataset_name=_dataset_config.name)
    _manager = RemoteDatasetPipeline(dataset=_dataset_config)

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.info("----- (2) Load metadata from previous run -----")
    logger.warning("Not yet implemented")
    _previous_metadata = None

    # ============================================================
    # 3) Ingest stage
    # ============================================================
    logger.info("----- (3) Ingest stage -----")

    try:
        _ingest_result = _manager.ingest(version=_version, previous_metadata=_previous_metadata)
    except IngestStageError as error:
        error.log(logger.critical)
        sys.exit(-1)

    assert not isinstance(_ingest_result, bool), "todo: charger métadonnées des runs"
    logger.info("Download stage completed !", **_ingest_result.model_dump(mode="json"))

    # ============================================================
    # 3.bis) (optional) Extract stage
    # ============================================================
    if _dataset_config.source.format.is_archive:
        logger.info("----- (3.bis) Extract stage -----")
        try:
            _extract_result = _manager.extract_archive(ingest_result=_ingest_result)
        except ExtractStageError as error:
            error.log(logger.critical)
            sys.exit(-1)

        logger.info("Extraction stage completed !", **_extract_result.model_dump(mode="json"))

        _should_skip = _manager.should_skip_extraction(
            extract_result=_extract_result, previous_metadata=_previous_metadata
        )

        if _should_skip:
            logger.info("Stopping pipeline.")  # TODO: ajout raison
            sys.exit(0)
    # TODO: trouver comment retirer ceci
    else:
        _extract_result = None

    # ============================================================
    # 4) Bronze stage
    # ============================================================
    logger.info("----- (4) Bronze stage -----")
    try:
        _bronze_result = _manager.to_bronze(
            ingest_or_extract_result=_extract_result if _extract_result else _ingest_result
        )
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info("Bronze stage completed !", **_bronze_result.model_dump(mode="json"))

    # ============================================================
    # 5) Silver stage
    # ============================================================
    logger.info("----- (5) Silver stage -----")
    try:
        _silver_result = _manager.to_silver(bronze_result=_bronze_result)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info("Silver stage completed !", **_silver_result.model_dump(mode="json"))

    # ============================================================
    # 6) Save successful run metadata
    # ============================================================
    logger.info("----- (6) Save successful run metadata -----")
    logger.warning("Not yet implemented")
