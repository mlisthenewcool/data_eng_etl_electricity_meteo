"""CLI entrypoint for local pipeline execution."""

# /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.

import sys
from datetime import UTC, datetime

import psycopg
import typer

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DataCatalogError,
    ExtractStageError,
    IngestStageError,
    PostgresLoadError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.postgres import (
    load_silver_to_postgres,
    open_standalone_connection,
)
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline

logger = get_logger("main")

app = typer.Typer(add_completion=False)


@app.command()
def main(dataset_name: str) -> None:  # noqa: PLR0911, PLR0912, PLR0915
    """Run the remote dataset pipeline for a single dataset.

    DATASET_NAME is the catalog identifier (e.g. odre_installations).
    """
    start_datetime = datetime.now(tz=UTC)

    # ============================================================
    # 0) Load catalog and dataset configuration
    # ============================================================
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
        dataset_config = catalog.get_remote_dataset(dataset_name)
    except DataCatalogError as error:
        error.log(logger.critical)
        sys.exit(1)

    logger.debug(
        "Data catalog loaded",
        remote_datasets=[dataset.name for dataset in catalog.get_remote_datasets()],
        gold_datasets=[dataset.name for dataset in catalog.get_gold_datasets()],
    )

    logger.debug(
        "Dataset config loaded",
        dataset_name=dataset_name,
        dataset_type=type(dataset_config).__name__,
        **dataset_config.model_dump(mode="json", exclude={"name"}),
    )

    # ============================================================
    # 1) Prepare version and RemoteIngestionPipeline
    # ============================================================
    version = dataset_config.ingestion.frequency.format_datetime_as_version(start_datetime)
    manager = RemoteIngestionPipeline(dataset=dataset_config)

    # ============================================================
    # 2) Load metadata from previous run (not-implemented yet)
    # ============================================================
    logger.warning("Load previous run metadata not yet implemented outside of Airflow")
    previous_snapshot = None

    # ============================================================
    # 3) Ingest stage
    # ============================================================
    try:
        ingest_ctx = manager.ingest(version=version, previous_snapshot=previous_snapshot)
    except IngestStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    if ingest_ctx is None:
        logger.info("Pipeline skipped: content unchanged")
        return

    # ============================================================
    # 4) (optional) Extract stage
    # ============================================================
    if dataset_config.source.format.is_archive:
        try:
            extract_ctx = manager.extract_archive(
                context=ingest_ctx, previous_snapshot=previous_snapshot
            )
        except ExtractStageError as error:
            error.log(logger.critical)
            sys.exit(1)

        if extract_ctx is None:
            logger.info("Pipeline skipped: extracted content unchanged")
            return
    else:
        logger.info("Extraction skipped: format is not archive", dataset_name=dataset_name)
        extract_ctx = None

    # ============================================================
    # 5) Bronze stage
    # ============================================================
    try:
        bronze_ctx = manager.to_bronze(context=extract_ctx or ingest_ctx)
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    # ============================================================
    # 6) Silver stage
    # ============================================================
    try:
        _ = manager.to_silver(context=bronze_ctx)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    # ============================================================
    # 7) Save successful run metadata
    # ============================================================
    logger.warning("Save run metadata not yet implemented outside of Airflow")

    # ============================================================
    # 8) Load data to Postgres
    # ============================================================
    try:
        connection = open_standalone_connection()
    except PostgresLoadError as err:
        err.log(logger.critical)
        sys.exit(1)
    except psycopg.OperationalError as err:
        logger.critical("Postgres connection failed", error=str(err))
        sys.exit(1)

    try:
        with connection:
            metrics = load_silver_to_postgres(dataset_config=dataset_config, conn=connection)
    except PostgresLoadError as err:
        err.log(logger.exception)
        sys.exit(1)

    logger.info("Load to Postgres ok", **metrics.model_dump())


if __name__ == "__main__":
    app()
