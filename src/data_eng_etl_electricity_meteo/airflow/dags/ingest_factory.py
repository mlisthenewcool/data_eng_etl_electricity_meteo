"""DAG factory for remote dataset ingestion pipelines.

Generates one Airflow DAG per remote dataset declared in the data catalog.
Each DAG follows the medallion architecture: landing -> bronze -> silver,
with optional archive extraction between landing and bronze.
"""

from collections.abc import Generator
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
from data_eng_etl_electricity_meteo.pipeline.remote_ingestion import RemoteIngestionPipeline
from data_eng_etl_electricity_meteo.pipeline.types import (
    PipelineContext,
    PipelineRunSnapshot,
)

if TYPE_CHECKING:
    from airflow.sdk.execution_time.context import InletEventsAccessors

logger = get_logger("dag_factory.ingest")


def _get_previous_snapshot(
    inlet_events: "InletEventsAccessors | None", asset: Asset
) -> PipelineRunSnapshot | None:
    """Extract the previous run snapshot from Airflow inlet events.

    Parameters
    ----------
    inlet_events:
        Inlet events accessor injected by Airflow, or ``None``.
    asset:
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


TASK_DOWNLOAD = "download_to_landing"
TASK_EXTRACT = "extract_archive_to_landing"
TASK_BRONZE = "convert_landing_to_bronze"
TASK_SILVER = "clean_bronze_to_silver"

# Task-specific timeouts (override defaults)
TASK_DOWNLOAD_TIMEOUT = timedelta(minutes=30)
TASK_EXTRACT_TIMEOUT = timedelta(minutes=5)
TASK_BRONZE_TIMEOUT = timedelta(minutes=3)
TASK_SILVER_TIMEOUT = timedelta(minutes=10)


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
        dag_id=f"{manager.dataset.name}_ingest",
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
        # ------------------------------------------------------------
        # 1. DOWNLOAD WITH SHORT-CIRCUITS IF CONTENT IS UNCHANGED
        #   HTTP HEAD - short-circuit with ETag / Last-modified
        #   DOWNLOAD
        #   COMPARE HASH (SHA256)
        # ------------------------------------------------------------
        @task.short_circuit(
            task_id=TASK_DOWNLOAD, execution_timeout=TASK_DOWNLOAD_TIMEOUT, inlets=[asset]
        )
        def download_task(
            version: str, inlet_events: "InletEventsAccessors | None" = None
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

        # ------------------------------------------------------------
        # 2. (optional) EXTRACTION - only if remote data requires it
        # ------------------------------------------------------------
        @task.short_circuit(
            task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT, inlets=[asset]
        )
        def extract_task(
            ctx: XComArg, inlet_events: "InletEventsAccessors | None" = None
        ) -> XComArg | bool:
            """Extract archive; short-circuit if SHA256 is unchanged."""
            previous_snapshot = _get_previous_snapshot(inlet_events, asset)

            context = manager.extract_archive(
                PipelineContext.model_validate(ctx), previous_snapshot=previous_snapshot
            )

            if context is None:
                return False

            return context.model_dump(mode="json")

        # ------------------------------------------------------------
        # 3. CONVERT TO BRONZE - parquet + column names normalisation
        # ------------------------------------------------------------
        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to versioned bronze Parquet."""
            result = manager.to_bronze(PipelineContext.model_validate(ctx))
            return result.model_dump(mode="json")

        # ------------------------------------------------------------
        # 4. TRANSFORM TO SILVER - Business Rules + Metadata
        # ------------------------------------------------------------
        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[asset])
        def silver_task(ctx: XComArg) -> Generator[Metadata]:
            """Apply business transformations and emit Asset metadata."""
            silver_result = manager.to_silver(PipelineContext.model_validate(ctx))

            # Emit enriched metadata for Airflow UI
            # These metadata fields will be visible in the Assets tab
            # and will be used to short-circuit refresh on download_task

            yield Metadata(
                asset=asset,
                # exclude={"path", "extracted_file_path"}
                extra=PipelineRunSnapshot.from_context(silver_result).model_dump(exclude_none=True),
            )

        # ------------------------------------------------------------
        # DAG's WORKFLOW : download > (extract) > bronze > silver
        # ------------------------------------------------------------

        # Version generated inside decorated function so Jinja template is resolved
        run_version = manager.dataset.ingestion.frequency.get_airflow_version_template()

        # 1. DOWNLOAD
        landing_ctx = download_task(run_version)

        # 2. (optional) EXTRACT
        if manager.dataset.source.format.is_archive:
            landing_ctx = extract_task(landing_ctx)

        # 3. CONVERT TO BRONZE
        bronze_ctx = bronze_task(landing_ctx)

        # 4. TRANSFORM TO SILVER
        _ = silver_task(bronze_ctx)

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
        e.log(logger.exception)
        return {}

    pipelines: dict[str, DAG] = {}

    for dataset in catalog.get_remote_datasets():
        try:
            asset = get_silver_file_asset(dataset.name)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset_name=dataset.name)
            continue

        try:
            manager = RemoteIngestionPipeline(dataset=dataset)
        except TransformNotFoundError as error:
            error.log(logger.warning)
            continue

        pipelines[dataset.name] = _create_dag(manager, asset)
        logger.info("Created dataset DAG", dag_id=f"{dataset.name}_ingest")

    return pipelines


# Note: expose DAGs to Airflow
_generate_all_dags()
