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
    assert isinstance(_dataset_config, RemoteDatasetConfig), "todo: créer get_remote_dataset"
    _version = _dataset_config.ingestion.frequency.format_datetime_as_version(_start_datetime)
    _manager = RemoteDatasetPipeline(dataset=_dataset_config)

    logger.info(
        "--- (1) Pipeline initialized",
        version=_version,
        **_dataset_config.model_dump(mode="json", include={"source": {"url"}}),
    )

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.warning("--- (2) Load previous run metadata not yet implemented outside of Airflow")
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
    logger.info(
        "--- (3) Download stage completed",
        **_ingest_result.model_dump(mode="json", include={"download_info": {"path", "size_mib"}}),
    )

    # ============================================================
    # 4) (optional) Extract stage
    # ============================================================
    if _dataset_config.source.format.is_archive:
        try:
            _extract_result = _manager.extract_archive(ingest_result=_ingest_result)
        except ExtractStageError as error:
            error.log(logger.critical)
            sys.exit(-1)

        assert _extract_result.extraction_info is not None, "todo: info de l'extraction absentes"
        logger.info(
            "--- (4) Extraction stage completed",
            **_extract_result.model_dump(
                mode="json", include={"extraction_info": {"file_path", "size_mib"}}
            ),
        )
        # file_path = _extract_result.extraction_info.file_path,
        # size_mib = _extract_result.extraction_info.size_mib,

        _should_skip = _manager.should_skip_extraction(
            extract_result=_extract_result, previous_metadata=_previous_metadata
        )

        if _should_skip:
            logger.info("Stopping pipeline: extracted content unchanged")
            sys.exit(0)
    else:
        logger.info("--- (4) Extraction stage skipped : dataset.source.format is not archive")
        _extract_result = None

    # ============================================================
    # 5) Bronze stage
    # ============================================================
    try:
        _bronze_result = _manager.to_bronze(
            ingest_or_extract_result=_extract_result if _extract_result else _ingest_result
        )
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info(
        "--- (5) Bronze stage completed",
        **_bronze_result.model_dump(mode="json", include={"bronze"}),
    )

    # ============================================================
    # 6) Silver stage
    # ============================================================
    try:
        _silver_result = _manager.to_silver(bronze_result=_bronze_result)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(-1)
    logger.info(
        "--- (6) Silver stage completed",
        **_silver_result.model_dump(mode="json", include={"silver"}),
    )
    # ============================================================
    # 7) Save successful run metadata
    # ============================================================
    logger.warning("--- (7) Save run metadata not yet implemented outside of Airflow")
