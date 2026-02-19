"""CLI entrypoint for local pipeline execution."""

import sys
from datetime import UTC, datetime

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
from data_eng_etl_electricity_meteo.pipeline.stage_types import PipelineContext

logger = get_logger("main")

if __name__ == "__main__":
    # ==================================================================================
    # /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ==================================================================================
    _DATASET_NAME = "ign_contours_iris"
    # _DATASET_NAME = "odre_eco2mix_tr"

    _start_datetime = datetime.now(tz=UTC)

    # ============================================================
    # 0) Load catalog and dataset configuration
    # ============================================================
    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.debug(
        "Data catalog loaded",
        remote_datasets=[dataset.name for dataset in _catalog.get_remote_datasets()],
        derived_datasets=[dataset.name for dataset in _catalog.get_derived_datasets()],
    )

    try:
        _dataset_config = _catalog.get_dataset(_DATASET_NAME)
    except DatasetNotFoundError as error:
        error.log(logger.critical)
        sys.exit(-1)

    logger.debug(
        "Dataset config loaded",
        dataset_name=_DATASET_NAME,
        dataset_type=type(_dataset_config).__name__,
        **_dataset_config.model_dump(mode="json", exclude={"name"}),
    )

    # ============================================================
    # 1) Prepare version and RemoteDatasetPipeline
    # ============================================================
    if not isinstance(_dataset_config, RemoteDatasetConfig):
        logger.critical(
            "Expected a remote dataset",
            dataset_name=_DATASET_NAME,
            actual_type=type(_dataset_config).__name__,
        )
        sys.exit(-1)

    assert isinstance(_dataset_config, RemoteDatasetConfig)
    _remote_config = _dataset_config
    _version = _remote_config.ingestion.frequency.format_datetime_as_version(_start_datetime)
    _manager = RemoteDatasetPipeline(dataset=_remote_config)

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.warning("Load previous run metadata not yet implemented outside of Airflow")
    _previous_metadata = None

    # ============================================================
    # 3) Ingest stage
    # ============================================================
    try:
        _ingest_result = _manager.ingest(version=_version, previous_metadata=_previous_metadata)
    except IngestStageError as error:
        error.log(logger.critical)
        sys.exit(-1)

    if _ingest_result is None:
        logger.info("Pipeline skipped: content unchanged")
        sys.exit(0)

    assert _ingest_result is not None
    _ingest_context = _ingest_result

    # ============================================================
    # 4) (optional) Extract stage
    # ============================================================
    _bronze_input: PipelineContext = _ingest_context

    if _remote_config.source.format.is_archive:
        try:
            _extract_result = _manager.extract_archive(
                context=_ingest_context, previous_metadata=_previous_metadata
            )
        except ExtractStageError as error:
            error.log(logger.critical)
            sys.exit(-1)

        if _extract_result is None:
            logger.info("Pipeline skipped: extracted content unchanged")
            sys.exit(0)

        assert _extract_result is not None
        _bronze_input = _extract_result
    else:
        logger.info("Extraction skipped: format is not archive", dataset_name=_DATASET_NAME)

    # ============================================================
    # 5) Bronze stage
    # ============================================================
    try:
        _bronze_result = _manager.to_bronze(context=_bronze_input)
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(-1)

    # ============================================================
    # 6) Silver stage
    # ============================================================
    try:
        _silver_result = _manager.to_silver(context=_bronze_result)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(-1)

    # ============================================================
    # 7) Save successful run metadata
    # ============================================================
    logger.warning("Save run metadata not yet implemented outside of Airflow")
