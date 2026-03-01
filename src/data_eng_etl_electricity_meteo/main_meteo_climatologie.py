"""CLI entrypoint for Météo France climatologie pipeline.

Custom pipeline that downloads 95 departmental data files from data.gouv.fr (Parquet
Hydra when available, CSV.gz fallback), merges them, then runs the standard bronze ->
silver -> Postgres flow via ``RemoteIngestionPipeline`` with a custom download.

Cannot use ``main_remote_dataset.py`` because the standard download expects a single
URL, whereas climatologie requires fetching and merging 95 separate files.

Usage::

    uv run --env-file=.env.local main_meteo_climatologie.py
    uv run --env-file=.env.local main_meteo_climatologie.py --skip-postgres
    uv run --env-file=.env.local main_meteo_climatologie.py \
        --year-start 2024 --year-end 2025
"""

import sys
from datetime import UTC, datetime
from functools import partial

import typer

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DataCatalogError,
    DownloadStageError,
    PostgresLoadError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.pg_loader import run_standalone_postgres_load
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline
from data_eng_etl_electricity_meteo.utils.meteo_download import download_climatologie

logger = get_logger("main.meteo_climatologie")

DATASET_NAME = "meteo_france_climatologie"

app = typer.Typer(add_completion=False)


@app.command()
def main(
    year_start: int | None = typer.Option(None, help="Start year (default: current - 1)"),
    year_end: int | None = typer.Option(None, help="End year (default: current)"),
    skip_postgres: bool = typer.Option(False, help="Skip Postgres loading"),
) -> None:
    """Run the Météo France climatologie pipeline.

    download -> bronze -> silver -> Postgres.
    """
    start_datetime = datetime.now(tz=UTC)

    # ------------------------------------------------------------------
    # 0) Load catalog and dataset configuration
    # ------------------------------------------------------------------
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
        dataset_config = catalog.get_remote_dataset(DATASET_NAME)
    except DataCatalogError as error:
        error.log(logger.critical)
        sys.exit(1)

    version = dataset_config.ingestion.frequency.format_datetime_as_version(start_datetime)

    # ------------------------------------------------------------------
    # 1) Build pipeline with custom download (95 departments → merged parquet)
    # ------------------------------------------------------------------
    manager = RemoteIngestionPipeline(
        dataset=dataset_config,
        custom_download=partial(
            download_climatologie,
            year_start=year_start,
            year_end=year_end,
        ),
    )

    # ------------------------------------------------------------------
    # 2) Download (custom: 95 departments → merged parquet in landing/)
    # ------------------------------------------------------------------
    try:
        download_ctx = manager.download(version=version, previous_snapshot=None)
    except DownloadStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    if download_ctx is None:
        logger.info("Pipeline skipped: content unchanged")
        return

    # ------------------------------------------------------------------
    # 3) Bronze (standard: landing → versioned parquet)
    # ------------------------------------------------------------------
    try:
        bronze_ctx = manager.to_bronze(context=download_ctx)
    except BronzeStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4) Silver (standard: bronze → business transform → current.parquet)
    # ------------------------------------------------------------------
    try:
        _ = manager.to_silver(context=bronze_ctx)
    except SilverStageError as error:
        error.log(logger.critical)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 5) Load to Postgres
    # ------------------------------------------------------------------
    if skip_postgres:
        logger.info("Postgres loading skipped (--skip-postgres)")
        return

    try:
        metrics = run_standalone_postgres_load(dataset_config)
    except PostgresLoadError as err:
        err.log(logger.critical)
        sys.exit(1)

    logger.info("Load to Postgres ok", **metrics.model_dump())


if __name__ == "__main__":
    app()
