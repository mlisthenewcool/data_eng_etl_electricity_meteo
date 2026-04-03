"""Shared pipeline runner for CLI entrypoints.

Encapsulates the sequential pipeline logic
(download -> extract -> bronze -> silver -> Postgres) so that CLI wrappers remain thin.

Warnings
--------
Do **not** raise ``SystemExit`` when running on Airflow. Let Airflow handle exceptions.
"""

import time
from datetime import UTC, datetime

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
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
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline
from data_eng_etl_electricity_meteo.pipeline.state import load_local_snapshot, save_local_snapshot
from data_eng_etl_electricity_meteo.pipeline.strategies import get_strategy
from data_eng_etl_electricity_meteo.pipeline.types import DownloadStrategy, PipelineRunSnapshot

logger = get_logger("cli")


def _load_postgres(dataset: RemoteDatasetConfig) -> None:
    """Run the Postgres load step, exit on failure.

    Parameters
    ----------
    dataset
        Remote dataset configuration from the catalog.
    """
    try:
        _ = run_standalone_postgres_load(dataset)
    except PostgresLoadError as err:
        err.log(logger.critical)
        raise SystemExit(1) from None


def run_pipeline(
    dataset_name: str,
    *,
    strategy: DownloadStrategy | None = None,
    skip_postgres: bool = False,
    only_postgres: bool = False,
) -> None:
    """Run the full remote-ingestion pipeline for a single dataset.

    Parameters
    ----------
    dataset_name
        Catalog identifier (e.g. ``odre_installations``).
    strategy
        Download strategy. When ``None``, uses ``get_strategy(dataset_name)``.
    skip_postgres
        When ``True``, skip the final Postgres load step.
    only_postgres
        When ``True``, skip ingestion and only run the Postgres load from the existing
        silver Parquet.
    """
    t0 = time.monotonic()

    # -- Load catalog and dataset configuration ----------------------------------------

    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
        dataset = catalog.get_remote_dataset(dataset_name)
    except DataCatalogError as error:
        error.log(logger.critical)
        raise SystemExit(1) from None

    if only_postgres:
        _load_postgres(dataset)
        return None

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

    version = dataset.ingestion.frequency.format_datetime_as_version(datetime.now(tz=UTC))
    resolved_strategy = strategy if strategy is not None else get_strategy(dataset_name)
    manager = RemoteIngestionPipeline(dataset=dataset, strategy=resolved_strategy)

    # -- Load previous run state -------------------------------------------------------

    previous_snapshot = load_local_snapshot(dataset_name)

    # -- Download ----------------------------------------------------------------------

    try:
        download_ctx = manager.download(version, previous_snapshot=previous_snapshot)
    except DownloadStageError as error:
        error.log(logger.critical)
        raise SystemExit(1) from None

    if download_ctx is None:
        return None

    # -- Extract (optional) ------------------------------------------------------------

    extract_ctx = None

    if dataset.source.format.is_archive:
        try:
            extract_ctx = manager.extract_archive(download_ctx, previous_snapshot=previous_snapshot)
        except ExtractStageError as error:
            error.log(logger.critical)
            raise SystemExit(1) from None

        # extract_archive returns None when the extracted content hash is
        # unchanged (smart-skip). Landing files are already cleaned up.
        if extract_ctx is None:
            return None
    else:
        logger.debug("Extraction skipped: format is not archive")

    # -- Convert to Bronze -------------------------------------------------------------

    try:
        bronze_ctx = manager.to_bronze(extract_ctx or download_ctx)
    except BronzeStageError as error:
        error.log(logger.critical)
        raise SystemExit(1) from None

    # -- Silver ------------------------------------------------------------------------

    try:
        silver_ctx = manager.to_silver(bronze_ctx)
    except SilverStageError as error:
        error.log(logger.critical)
        raise SystemExit(1) from None

    # -- Save run state ----------------------------------------------------------------

    save_local_snapshot(dataset_name, snapshot=PipelineRunSnapshot.from_context(silver_ctx))

    # -- Postgres load -----------------------------------------------------------------

    if not skip_postgres:
        _load_postgres(dataset)
    else:
        logger.info("Postgres load skipped: --skip-postgres")

    logger.info("Pipeline completed", duration_s=round(time.monotonic() - t0, 2))

    return None
