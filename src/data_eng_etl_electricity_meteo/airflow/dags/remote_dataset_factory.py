"""DAG factory for remote dataset ingestion pipelines.

Generates one Airflow DAG per remote dataset declared in the data catalog.
Each DAG follows the medallion architecture: landing -> bronze -> silver,
with optional archive extraction between landing and bronze.
"""

from collections.abc import Generator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, Metadata, XComArg, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_asset
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.exceptions import (
    InvalidCatalogError,
    TransformNotFoundError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.remote_dataset_manager import RemoteDatasetPipeline
from data_eng_etl_electricity_meteo.pipeline.stage_types import (
    BronzeStageResult,
    DownloadStageResult,
)

if TYPE_CHECKING:
    from airflow.sdk.execution_time.context import InletEventsAccessors

factory_logger = get_logger("dag_factory")

# =============================================================================
# Production defaults - TODO, move to settings
# =============================================================================

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,  # DAGs freeze if previous run failed
    # max_active_runs controlled globally via
    # AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
    # Allows inter-DAG parallelism, prevents intra-DAG file conflicts
}

TASK_INGEST = "ingest_remote_to_landing"
TASK_EXTRACT = "extract_archive_to_landing"
TASK_BRONZE = "convert_landing_to_bronze"
TASK_SILVER = "clean_bronze_to_silver"

# Task-specific timeouts (override defaults)
TASK_INGEST_TIMEOUT = timedelta(minutes=30)
TASK_EXTRACT_TIMEOUT = timedelta(minutes=5)
TASK_BRONZE_TIMEOUT = timedelta(minutes=3)
TASK_SILVER_TIMEOUT = timedelta(minutes=10)


def _create_dag(manager: RemoteDatasetPipeline, asset: Asset) -> DAG:
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
        dag_id=f"ingest_{manager.dataset.name}",
        schedule=manager.dataset.ingestion.frequency.airflow_schedule,
        start_date=datetime(2026, 1, 24),
        catchup=False,  # TODO[prod]: set to True
        default_args=DEFAULT_ARGS,
        tags=[manager.dataset.source.provider, "ingestion"],
        description=manager.dataset.description[:200] if manager.dataset.description else None,
        doc_md=__doc__,
        # max_active_runs=1 controlled globally
    )
    def _dag() -> None:
        # ============================================================
        # 2. INGEST WITH SHORT-CIRCUITS IF CONTENT IN UNCHANGED
        #   HTTP HEAD - short-circuit with ETag / Last-modified
        #   DOWNLOAD
        #   COMPARE HASH (SHA256)
        # ============================================================
        @task.short_circuit(
            task_id=TASK_INGEST, execution_timeout=TASK_INGEST_TIMEOUT, inlets=[asset]
        )
        def ingest_task(
            version: str, inlet_events: "InletEventsAccessors | None" = None
        ) -> XComArg | bool:
            """Ingest remote data to landing with short-circuit if unchanged.

            1. Retrieve metadata from remote.
            2. Compare to previous run metadata — short-circuit if unchanged.
            3. Download content to landing layer.
            4. Compare SHA-256 hashes — short-circuit if identical.
            """
            previous_metadata = (
                inlet_events[asset][-1].extra
                if inlet_events and asset in inlet_events and len(inlet_events[asset]) > 0
                else None
            )

            ingestion_result = manager.ingest(version=version, previous_metadata=previous_metadata)

            if isinstance(ingestion_result, bool):
                return False

            return ingestion_result.model_dump(mode="json")

        # ============================================================
        # 3. (Optional) EXTRACTION - only if remote data requires it
        # ============================================================
        @task.short_circuit(
            task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT, inlets=[asset]
        )
        def extract_task(
            ctx: XComArg, inlet_events: "InletEventsAccessors | None" = None
        ) -> XComArg | bool:
            """Extract archive; short-circuit if SHA256 is unchanged."""
            previous_metadata = (
                inlet_events[asset][-1].extra
                if inlet_events and asset in inlet_events and len(inlet_events[asset]) > 0
                else None
            )

            extract_result = manager.extract_archive(DownloadStageResult.model_validate(ctx))

            # If hash is identical, remove landing files and skip
            if manager.should_skip_extraction(
                extract_result=extract_result, previous_metadata=previous_metadata
            ):
                return False

            return extract_result.model_dump(mode="json")

        # ============================================================
        # 4. CONVERT TO BRONZE - parquet + column names normalisation
        # ============================================================
        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to versioned bronze Parquet."""
            result = manager.to_bronze(DownloadStageResult.model_validate(ctx))
            return result.model_dump(mode="json")

        # ============================================================
        # 5. TRANSFORM TO SILVER - Business Rules + Metadata
        # ============================================================
        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[asset])
        def silver_task(ctx: XComArg) -> Generator[Metadata]:
            """Apply business transformations and emit Asset metadata."""
            silver_result = manager.to_silver(BronzeStageResult.model_validate(ctx))

            # Emit enriched metadata for Airflow UI
            # These metadata fields will be visible in the Assets tab
            # and will be used to short-circuit refresh on ingest_task

            yield Metadata(
                asset=asset,
                # exclude={"path", "extracted_file_path"}
                extra=manager.create_metadata_emission(silver_result).model_dump(exclude_none=True),
            )

        # ============================================================
        # DAG's WORKFLOW : ingest > (extract) > bronze > silver
        # ============================================================

        # Version generated inside decorated function so Jinja template is resolved
        run_version = manager.dataset.ingestion.frequency.get_airflow_version_template()

        # 1. INGEST
        landing_ctx = ingest_task(run_version)

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
        e.log(factory_logger.exception)
        return {}

    pipelines: dict[str, DAG] = {}

    for dataset in catalog.get_remote_datasets():
        try:
            asset = get_asset(dataset.name, "silver")
            manager = RemoteDatasetPipeline(dataset=dataset)
            pipelines[dataset.name] = _create_dag(manager, asset)
            factory_logger.info("Created dataset DAG", dag_id=f"ingest_{dataset.name}")
        # TODO: catch les exceptions au bon endroit
        except TransformNotFoundError as error:
            error.log(factory_logger.warning)
        except ValueError:
            factory_logger.exception(
                "Failed to create DAG.",
                dataset_name=dataset.name,
            )

    return pipelines


# Note: expose DAGs to Airflow
_generate_all_dags()
