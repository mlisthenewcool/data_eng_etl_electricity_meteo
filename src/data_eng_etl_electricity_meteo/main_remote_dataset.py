"""CLI entrypoint for local pipeline execution."""

# /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.

import sys
from datetime import UTC, datetime

import psycopg

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DatasetNotFoundError,
    ExtractStageError,
    IngestStageError,
    InvalidCatalogError,
    PostgresLoadError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.postgres import (
    load_to_silver,
    open_standalone_connection,
)
from data_eng_etl_electricity_meteo.pipeline.remote_dataset_manager import RemoteDatasetPipeline

logger = get_logger("main")

_DATASET_NAME = "ign_contours_iris"
# _DATASET_NAME = "odre_eco2mix_tr"
# _DATASET_NAME = "odre_installations"


def main() -> int:  # noqa: PLR0911, PLR0912, PLR0915
    """Run the remote dataset pipeline for a single dataset.

    Returns
    -------
    int
        Exit code: 0 on success or skip, -1 on error.
    """
    start_datetime = datetime.now(tz=UTC)

    # ============================================================
    # 0) Load catalog and dataset configuration
    # ============================================================
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as error:
        error.log(logger.critical)
        return -1

    logger.debug(
        "Data catalog loaded",
        remote_datasets=[dataset.name for dataset in catalog.get_remote_datasets()],
        derived_datasets=[dataset.name for dataset in catalog.get_derived_datasets()],
    )

    try:
        dataset_config = catalog.get_dataset(_DATASET_NAME)
    except DatasetNotFoundError as error:
        error.log(logger.critical)
        return -1

    logger.debug(
        "Dataset config loaded",
        dataset_name=_DATASET_NAME,
        dataset_type=type(dataset_config).__name__,
        **dataset_config.model_dump(mode="json", exclude={"name"}),
    )

    # ============================================================
    # 1) Prepare version and RemoteDatasetPipeline
    # ============================================================
    if not isinstance(dataset_config, RemoteDatasetConfig):
        logger.critical(
            "Expected a remote dataset",
            dataset_name=_DATASET_NAME,
            actual_type=type(dataset_config).__name__,
        )
        return -1

    version = dataset_config.ingestion.frequency.format_datetime_as_version(start_datetime)
    manager = RemoteDatasetPipeline(dataset=dataset_config)

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.warning("Load previous run metadata not yet implemented outside of Airflow")
    previous_metadata = None

    # ============================================================
    # 3) Ingest stage
    # ============================================================
    try:
        ingest_ctx = manager.ingest(version=version, previous_metadata=previous_metadata)
    except IngestStageError as error:
        error.log(logger.critical)
        return -1

    if ingest_ctx is None:
        logger.info("Pipeline skipped: content unchanged")
        return 0

    # ============================================================
    # 4) (optional) Extract stage
    # ============================================================
    if dataset_config.source.format.is_archive:
        try:
            extract_ctx = manager.extract_archive(
                context=ingest_ctx, previous_metadata=previous_metadata
            )
        except ExtractStageError as error:
            error.log(logger.critical)
            return -1

        if extract_ctx is None:
            logger.info("Pipeline skipped: extracted content unchanged")
            return 0
    else:
        logger.info("Extraction skipped: format is not archive", dataset_name=_DATASET_NAME)
        extract_ctx = None  # _extract_ctx is unbound in this branch : must be set explicitly

    # ============================================================
    # 5) Bronze stage
    # ============================================================
    try:
        bronze_ctx = manager.to_bronze(context=extract_ctx or ingest_ctx)
    except BronzeStageError as error:
        error.log(logger.critical)
        return -1

    # ============================================================
    # 6) Silver stage
    # ============================================================
    try:
        _ = manager.to_silver(context=bronze_ctx)
    except SilverStageError as error:
        error.log(logger.critical)
        return -1

    # ============================================================
    # 7) Save successful run metadata
    # ============================================================
    logger.warning("Save run metadata not yet implemented outside of Airflow")

    # ============================================================
    # 8) Load data to Postgres
    # ============================================================
    try:
        connection = open_standalone_connection()
    except (OSError, psycopg.OperationalError) as err:
        logger.error("PostgreSQL connection failed", error=str(err))
        return -1

    try:
        with connection:
            metrics = load_to_silver(dataset_config=dataset_config, conn=connection)
    except PostgresLoadError as err:
        err.log(logger.error)
        return -1

    logger.info("Load to Postgres ok", metrics=metrics)

    return 0


if __name__ == "__main__":
    sys.exit(main())
