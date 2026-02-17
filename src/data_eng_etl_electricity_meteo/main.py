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
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.remote_dataset_manager import RemoteDatasetPipeline

logger = get_logger("main")

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

    logger.info(
        "Data catalog loaded",
        stage=0,
        remote_datasets=[dataset.name for dataset in _catalog.get_remote_datasets()],
        derived_datasets=[dataset.name for dataset in _catalog.get_derived_datasets()],
    )

    try:
        _dataset_config = _catalog.get_dataset(_DATASET_NAME)
    except DatasetNotFoundError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.info(
        "Dataset config loaded",
        dataset_name=_DATASET_NAME,
        dataset_type=type(_dataset_config).__name__,
        **_dataset_config.model_dump(mode="json", exclude={"name"}),
    )

    # ============================================================
    # 1) Prepare version and RemoteDatasetPipeline
    # ============================================================
    assert isinstance(_dataset_config, RemoteDatasetConfig), "todo: créer get_remote_dataset"
    _version = _dataset_config.ingestion.frequency.format_datetime_as_version(_start_datetime)
    _manager = RemoteDatasetPipeline(dataset=_dataset_config)

    logger.info("Pipeline initialized", stage=1, version=_version)

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.warning("Previous run metadata not yet implemented", stage=2)
    _previous_metadata = None

    # ============================================================
    # 3) Ingest stage
    # ============================================================
    try:
        _ingest_result = _manager.ingest(version=_version, previous_metadata=_previous_metadata)
    except IngestStageError as error:
        error.log(logger.critical)
        sys.exit(-1)

    assert not isinstance(_ingest_result, bool), "todo: charger métadonnées des runs"
    logger.info("Download stage completed", stage=3, **_ingest_result.model_dump(mode="json"))

    # ============================================================
    # 3.bis) (optional) Extract stage
    # ============================================================
    if _dataset_config.source.format.is_archive:
        try:
            _extract_result = _manager.extract_archive(ingest_result=_ingest_result)
        except ExtractStageError as error:
            error.log(logger.critical)
            sys.exit(-1)

        logger.info(
            "Extraction stage completed", stage="3bis", **_extract_result.model_dump(mode="json")
        )

        _should_skip = _manager.should_skip_extraction(
            extract_result=_extract_result, previous_metadata=_previous_metadata
        )

        if _should_skip:
            logger.info("Stopping pipeline: extracted content unchanged")
            sys.exit(0)
    else:
        _extract_result = None

    # ============================================================
    # 4) Bronze stage
    # ============================================================
    try:
        _bronze_result = _manager.to_bronze(
            ingest_or_extract_result=_extract_result if _extract_result else _ingest_result
        )
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info("Bronze stage completed", stage=4, **_bronze_result.model_dump(mode="json"))

    # ============================================================
    # 5) Silver stage
    # ============================================================
    try:
        _silver_result = _manager.to_silver(bronze_result=_bronze_result)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info("Silver stage completed", stage=5, **_silver_result.model_dump(mode="json"))

    # ============================================================
    # 6) Save successful run metadata
    # ============================================================
    logger.warning("Save run metadata not yet implemented", stage=6)
