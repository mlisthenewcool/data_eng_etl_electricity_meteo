"""DAG factory for silver file production (source HTTP → silver file).

Generates one ``{dataset}_to_silver`` DAG per remote dataset in the data catalog.
Each DAG runs on its dataset's configured schedule and orchestrates: download →
(extract) → bronze → silver, producing a silver file Asset.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from airflow.sdk import DAG, Asset, Metadata, XComArg, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_silver_file_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    InvalidCatalogError,
    TransformNotFoundError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.custom_downloads import CUSTOM_DOWNLOADS
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline
from data_eng_etl_electricity_meteo.pipeline.types import (
    PipelineContext,
    PipelineRunSnapshot,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from airflow.sdk.execution_time.context import InletEventsAccessors

logger = get_logger("dag.to_silver")


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


TASK_DOWNLOAD = "download_to_landing"
TASK_EXTRACT = "extract_archive_to_landing"
TASK_BRONZE = "convert_landing_to_bronze"
TASK_SILVER = "clean_bronze_to_silver"

# Task-specific timeouts (override defaults)
TASK_DOWNLOAD_TIMEOUT = timedelta(minutes=30)
TASK_EXTRACT_TIMEOUT = timedelta(minutes=5)
TASK_BRONZE_TIMEOUT = timedelta(minutes=3)
TASK_SILVER_TIMEOUT = timedelta(minutes=10)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def _get_previous_snapshot(
    inlet_events: InletEventsAccessors | None, asset: Asset
) -> PipelineRunSnapshot | None:
    """Extract the previous run snapshot from Airflow inlet events.

    Parameters
    ----------
    inlet_events
        Inlet events accessor injected by Airflow, or ``None``.
    asset
        The Asset whose metadata to retrieve.

    Returns
    -------
    PipelineRunSnapshot | None
        Validated snapshot, or ``None`` if no previous event exists.
    """
    raw_metadata = (
        inlet_events[asset][-1].extra
        if inlet_events and asset in inlet_events and len(inlet_events[asset]) > 0
        else None
    )
    return PipelineRunSnapshot.from_metadata_dict(raw_metadata)


# --------------------------------------------------------------------------------------
# DAG factory
# --------------------------------------------------------------------------------------


def _create_dag(manager: RemoteIngestionPipeline, asset: Asset) -> DAG:
    """Create a production-ready DAG for a dataset.

    Parameters
    ----------
    manager
        Pipeline manager bound to a single remote dataset.
    asset
        Target Asset representing the silver layer output.

    Returns
    -------
    DAG
        Instantiated DAG object.
    """

    @dag(
        dag_id=f"{manager.dataset.name}_to_silver",
        schedule=manager.dataset.ingestion.frequency.airflow_schedule,
        start_date=START_DATE,
        catchup=False,  # TODO[prod]: set to True
        default_args=DEFAULT_ARGS,
        tags=[manager.dataset.source.provider, "ingestion"],
        description=manager.dataset.description[:200] if manager.dataset.description else None,
        doc_md=__doc__,
        # max_active_runs=1 controlled globally
    )
    def _dag() -> None:
        @task.short_circuit(
            task_id=TASK_DOWNLOAD, execution_timeout=TASK_DOWNLOAD_TIMEOUT, inlets=[asset]
        )
        def download_task(
            version: str, inlet_events: InletEventsAccessors | None = None
        ) -> XComArg | bool:
            """Download remote data to landing with short-circuit if unchanged.

            1. Retrieve metadata from remote.
            2. Compare to previous run metadata — short-circuit if unchanged.
            3. Download content to landing layer.
            4. Compare SHA-256 hashes — short-circuit if identical.
            """
            previous_snapshot = _get_previous_snapshot(inlet_events, asset)

            ingestion_result = manager.download(
                version=version, previous_snapshot=previous_snapshot
            )

            if ingestion_result is None:
                return False

            return ingestion_result.model_dump(mode="json")

        @task.short_circuit(
            task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT, inlets=[asset]
        )
        def extract_task(
            ctx: XComArg, inlet_events: InletEventsAccessors | None = None
        ) -> XComArg | bool:
            """Extract archive; short-circuit if SHA256 is unchanged."""
            previous_snapshot = _get_previous_snapshot(inlet_events, asset)

            context = manager.extract_archive(
                PipelineContext.model_validate(ctx), previous_snapshot=previous_snapshot
            )

            if context is None:
                return False

            return context.model_dump(mode="json")

        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to versioned bronze Parquet."""
            result = manager.to_bronze(PipelineContext.model_validate(ctx))
            return result.model_dump(mode="json")

        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[asset])
        def silver_task(ctx: XComArg) -> Generator[Metadata]:
            """Apply business transformations and emit Asset metadata."""
            silver_result = manager.to_silver(PipelineContext.model_validate(ctx))

            # Persisted in Asset metadata for smart-skip on next run

            yield Metadata(
                asset=asset,
                # exclude={"path", "extracted_file_path"}
                extra=PipelineRunSnapshot.from_context(silver_result).model_dump(exclude_none=True),
            )

        # -- DAG workflow: download > (extract) > bronze > silver ----------------------

        # Version generated inside decorated function so Jinja template is resolved
        run_version = manager.dataset.ingestion.frequency.get_airflow_version_template()

        landing_ctx = download_task(version=run_version)
        if manager.dataset.source.format.is_archive:
            landing_ctx = extract_task(ctx=landing_ctx)
        bronze_ctx = bronze_task(ctx=landing_ctx)
        _ = silver_task(ctx=bronze_ctx)

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """Generate ingestion DAGs for all remote datasets in the catalog.

    Returns
    -------
    dict[str, DAG]
        Mapping of dataset names to their ingestion DAG objects.
    """
    try:
        catalog = DataCatalog.load(path=settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    pipelines: dict[str, DAG] = {}

    for dataset in catalog.get_remote_datasets():
        try:
            asset = get_silver_file_asset(dataset.name)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset=dataset.name)
            continue  # move to next dataset

        try:
            manager = RemoteIngestionPipeline(
                dataset=dataset, custom_download=CUSTOM_DOWNLOADS.get(dataset.name)
            )
        except TransformNotFoundError as error:
            error.log(logger.warning)
            continue  # move to next dataset

        pipelines[dataset.name] = _create_dag(manager, asset)
        logger.info("to_silver DAG created", dataset=dataset.name)

    total = len(catalog.get_remote_datasets())
    logger.info("to_silver factory complete", created_count=len(pipelines), total_count=total)

    return pipelines


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
