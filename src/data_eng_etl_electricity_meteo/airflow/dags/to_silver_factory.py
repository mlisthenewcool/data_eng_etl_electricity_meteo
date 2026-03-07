"""DAG factory for silver file production (source HTTP → silver file).

Generates one ``{dataset}_to_silver`` DAG per remote dataset in the data catalog.
Each DAG runs on its dataset's configured schedule and orchestrates: download →
(extract) → bronze → silver, producing a silver file Asset.
"""

from collections.abc import Generator
from datetime import timedelta

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


def _create_dag(manager: RemoteIngestionPipeline, *, asset: Asset) -> DAG:
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
        @task.short_circuit(task_id=TASK_DOWNLOAD, execution_timeout=TASK_DOWNLOAD_TIMEOUT)
        def download_task(version: str) -> XComArg | bool:
            """Download remote data to landing with short-circuit if unchanged.

            1. Load previous run state from local JSON.
            2. Retrieve metadata from remote.
            3. Compare to previous run metadata — short-circuit if unchanged.
            4. Download content to landing layer.
            5. Compare SHA-256 hashes — short-circuit if identical.
            """
            previous_snapshot = load_local_snapshot(manager.dataset.name)

            ingestion_result = manager.download(
                version=version, previous_snapshot=previous_snapshot
            )

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
            result = manager.to_bronze(PipelineContext.model_validate(ctx))
            return result.model_dump(mode="json")

        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[asset])
        def silver_task(ctx: XComArg) -> Generator[Metadata]:
            """Apply business transformations, save state, and emit Asset metadata."""
            silver_result = manager.to_silver(PipelineContext.model_validate(ctx))
            snapshot = PipelineRunSnapshot.from_context(silver_result)

            # Persist to local JSON (single source of truth for smart-skip)
            save_local_snapshot(manager.dataset.name, snapshot=snapshot)

            # Emit Asset metadata (UI observability + downstream triggering)
            yield Metadata(
                asset=asset,
                extra=snapshot.model_dump(exclude_none=True),
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
            asset = get_silver_file_asset(dataset)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset=dataset.name)
            continue  # move to next dataset

        try:
            manager = RemoteIngestionPipeline(
                dataset=dataset,
                custom_download=CUSTOM_DOWNLOADS.get(dataset.name),
                custom_metadata=CUSTOM_METADATA.get(dataset.name),
            )
        except TransformNotFoundError as error:
            error.log(logger.warning)
            continue  # move to next dataset

        pipelines[dataset.name] = _create_dag(manager, asset=asset)
        logger.info("to_silver DAG created", dataset=dataset.name)

    total = len(catalog.get_remote_datasets())
    logger.info("to_silver factory completed", created_count=len(pipelines), total_count=total)

    return pipelines


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
