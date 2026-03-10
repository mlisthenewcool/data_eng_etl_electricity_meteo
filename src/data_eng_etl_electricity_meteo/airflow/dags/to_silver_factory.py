"""DAG factory for silver file production (source HTTP → silver file).

Generates one ``{dataset}_to_silver`` DAG per remote dataset in the data catalog.
Each DAG runs on its dataset's configured schedule and orchestrates: download →
(extract) → bronze → silver, producing a silver file Asset.
"""

from collections.abc import Generator
from datetime import timedelta

from airflow.sdk import DAG, Asset, Metadata, XComArg, dag, get_current_context, task

from data_eng_etl_electricity_meteo.airflow.assets import get_silver_file_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    InvalidCatalogError,
    TransformNotFoundError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.custom_downloads.registry import CUSTOM_DOWNLOADS
from data_eng_etl_electricity_meteo.custom_metadata.registry import CUSTOM_METADATA
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline
from data_eng_etl_electricity_meteo.pipeline.state import load_local_snapshot, save_local_snapshot
from data_eng_etl_electricity_meteo.pipeline.types import PipelineContext, PipelineRunSnapshot

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
# DAG factory
# --------------------------------------------------------------------------------------


def _create_dag(manager: RemoteIngestionPipeline, outlet: Asset) -> DAG:
    """Create a production-ready DAG for a dataset.

    Parameters
    ----------
    manager
        Pipeline manager bound to a single remote dataset.
    outlet
        Asset emitted after successful silver transformation.

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
        @task.short_circuit(task_id=TASK_DOWNLOAD, execution_timeout=TASK_DOWNLOAD_TIMEOUT)
        def download_task() -> XComArg | bool:
            """Download remote data to landing with short-circuit if unchanged.

            The version string is computed inside the task (via ``get_current_context``)
            rather than injected as a Jinja template. Airflow 3 Jinja rendering runs
            before the full context is populated, so ``logical_date`` / ``ds`` are
            undefined for manual triggers. Using the Python runtime context avoids this
            and is stable across Airflow versions.

            Notes
            -----
            1. Load previous run state from local JSON.
            2. Retrieve metadata from remote.
            3. Compare to previous run metadata — short-circuit if unchanged.
            4. Download content to landing layer.
            5. Compare SHA-256 hashes — short-circuit if identical.
            """
            # logical_date is None for manual triggers; run_after is always set.
            dag_run = get_current_context()["dag_run"]
            logical_date = dag_run.logical_date or dag_run.run_after
            version = manager.dataset.ingestion.frequency.format_datetime_as_version(logical_date)

            previous_snapshot = load_local_snapshot(manager.dataset.name)

            ingestion_result = manager.download(version, previous_snapshot=previous_snapshot)

            if ingestion_result is None:
                return False

            return ingestion_result.model_dump(mode="json")

        @task.short_circuit(task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT)
        def extract_task(ctx: XComArg) -> XComArg | bool:
            """Extract archive; short-circuit if SHA256 is unchanged."""
            previous_snapshot = load_local_snapshot(manager.dataset.name)

            context = manager.extract_archive(
                PipelineContext.model_validate(ctx), previous_snapshot=previous_snapshot
            )

            if context is None:
                return False

            return context.model_dump(mode="json")

        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to versioned bronze Parquet."""
            return manager.to_bronze(PipelineContext.model_validate(ctx)).model_dump(mode="json")

        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[outlet])
        def silver_task(ctx: XComArg) -> Generator[Metadata]:
            """Apply business transformations, save state, and emit Asset metadata."""
            silver_result = manager.to_silver(PipelineContext.model_validate(ctx))
            snapshot = PipelineRunSnapshot.from_context(silver_result)

            # Persist to local JSON (single source of truth for smart-skip)
            save_local_snapshot(manager.dataset.name, snapshot=snapshot)

            # Emit Asset metadata (UI observability + downstream triggering)
            yield Metadata(
                asset=outlet,
                extra=snapshot.model_dump(exclude_none=True),
            )

        # -- DAG workflow: download > (extract) > bronze > silver ----------------------

        landing_ctx = download_task()
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
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    remote_datasets = catalog.get_remote_datasets()
    pipelines: dict[str, DAG] = {}

    for dataset in remote_datasets:
        try:
            asset = get_silver_file_asset(dataset)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset_name=dataset.name)
            continue  # move to next dataset

        try:
            manager = RemoteIngestionPipeline(
                dataset=dataset,
                _custom_download=CUSTOM_DOWNLOADS.get(dataset.name),
                _custom_metadata=CUSTOM_METADATA.get(dataset.name),
            )
        except TransformNotFoundError as error:
            error.log(logger.warning)
            continue  # move to next dataset

        pipelines[dataset.name] = _create_dag(manager, outlet=asset)
        logger.debug("to_silver DAG created", dataset_name=dataset.name)

    logger.info(
        "to_silver factory completed",
        created_count=len(pipelines),
        total_count=len(remote_datasets),
    )

    return pipelines


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
