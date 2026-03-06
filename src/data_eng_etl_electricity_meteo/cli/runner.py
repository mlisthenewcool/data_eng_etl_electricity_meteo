"""Shared pipeline runner for CLI entrypoints.

Encapsulates the sequential pipeline logic
(download -> extract -> bronze -> silver -> Postgres) so that CLI wrappers remain thin.

Warnings
--------
Do **not** raise ``SystemExit`` when running on Airflow. Let Airflow handle exceptions.
"""

from datetime import UTC, datetime

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DataCatalogError,
    DownloadStageError,
    ExtractStageError,
    PostgresLoadError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.pg_loader import run_standalone_postgres_load
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import (
    CustomDownloadFunc,
    RemoteIngestionPipeline,
)

logger = get_logger("cli")


def run_pipeline(
    dataset_name: str,
    custom_download: CustomDownloadFunc | None = None,
    skip_postgres: bool = False,
) -> None:
    """Run the full remote-ingestion pipeline for a single dataset.

    Parameters
    ----------
    dataset_name
        Catalog identifier (e.g. ``odre_installations``).
    custom_download
        Optional callable replacing the standard single-URL download
        (e.g. multi-file merge for climatologie).
    skip_postgres
        When ``True``, skip the final Postgres load step.
    """
    start_datetime = datetime.now(tz=UTC)

    # -- Load catalog and dataset configuration ----------------------------------------

    try:
        catalog = DataCatalog.load(path=settings.data_catalog_file_path)
        dataset = catalog.get_remote_dataset(name=dataset_name)
    except DataCatalogError as error:
        error.log(logger.critical)
        raise SystemExit(1)

    logger.debug(
        "Data catalog loaded",
        remote_datasets=[ds.name for ds in catalog.get_remote_datasets()],
        gold_datasets=[ds.name for ds in catalog.get_gold_datasets()],
    )
    logger.debug(
        "Dataset config loaded",
        dataset_type=type(dataset).__name__,
        **dataset.model_dump(mode="json", exclude={"name"}),
    )

    # -- Prepare version and pipeline --------------------------------------------------

    version = dataset.ingestion.frequency.format_datetime_as_version(start_datetime)
    manager = RemoteIngestionPipeline(dataset=dataset, custom_download=custom_download)

    # -- Load previous run metadata (not implemented yet) ------------------------------

    logger.warning("Load previous run metadata not yet implemented outside of Airflow")

    # -- Download ----------------------------------------------------------------------

    try:
        download_ctx = manager.download(version=version, previous_snapshot=None)
    except DownloadStageError as error:
        error.log(logger.critical)
        raise SystemExit(1)

    if download_ctx is None:
        logger.info("Pipeline skipped: content unchanged")
        return

    # -- Extract (optional) ------------------------------------------------------------

    if dataset.source.format.is_archive:
        try:
            extract_ctx = manager.extract_archive(context=download_ctx, previous_snapshot=None)
        except ExtractStageError as error:
            error.log(logger.critical)
            raise SystemExit(1)

        if extract_ctx is None:
            logger.info("Pipeline skipped: extracted content unchanged")
            return
    else:
        logger.info("Extraction skipped: format is not archive")
        extract_ctx = None

    # -- Convert to Bronze -------------------------------------------------------------

    try:
        bronze_ctx = manager.to_bronze(context=extract_ctx or download_ctx)
    except BronzeStageError as error:
        error.log(logger.critical)
        raise SystemExit(1)

    # -- Silver ------------------------------------------------------------------------

    try:
        _ = manager.to_silver(context=bronze_ctx)
    except SilverStageError as error:
        error.log(logger.critical)
        raise SystemExit(1)

    # -- Save run metadata (not implemented yet) ---------------------------------------

    logger.warning("Save run metadata not yet implemented outside of Airflow")

    # -- Postgres load -----------------------------------------------------------------

    if skip_postgres:
        logger.info("Postgres loading skipped (--skip-postgres)")
        return

    try:
        _ = run_standalone_postgres_load(dataset)
    except PostgresLoadError as err:
        err.log(logger.critical)
        raise SystemExit(1)
