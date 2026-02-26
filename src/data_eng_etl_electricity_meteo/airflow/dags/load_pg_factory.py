"""DAG factory for loading silver parquet files into Postgres.

Generates one Airflow DAG per remote dataset declared in the data catalog.
Each DAG is triggered automatically when the upstream silver file Asset is
updated (i.e. when the corresponding ingestion DAG produces new data).

Pipeline position::

    [ingest_*]  →  silver file Asset  →  [load_pg_*]  →  silver PG Asset  →  [dbt]
       ETL                                  LOAD                               ELT

Each generated DAG:
- **Inlet** : silver file Asset (``file://`` URI to ``current.parquet``)
- **Task**  : load silver parquet → Postgres ``silver.{dataset}`` table
- **Outlet**: silver PG Asset (``postgres://project/silver.{dataset}``)

The load task uses an Airflow ``PostgresHook`` (connection id ``project_postgres``)
so credentials come from the Airflow connection store, not from
``open_standalone_connection()``.
"""

from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, Asset, Metadata, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_silver_file_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import InvalidCatalogError
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.postgres import load_silver_to_postgres_from_hook

logger = get_logger("dag_factory.load_pg")

# ---------------------------------------------------------------------------
# Production defaults (mirror remote_dataset_factory.py)
# ---------------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
}

TASK_LOAD = "load_silver_to_postgres"
TASK_LOAD_TIMEOUT = timedelta(minutes=20)


def _create_dag(
    dataset_config: RemoteDatasetConfig,
    silver_file_asset: Asset,
    silver_pg_asset: Asset,
) -> DAG:
    """Create a load DAG for a single dataset.

    Parameters
    ----------
    dataset_config:
        Remote dataset configuration from the catalog.
    silver_file_asset:
        Inlet: the silver ``file://`` Asset produced by the ingestion DAG.
    silver_pg_asset:
        Outlet: the silver Postgres Asset emitted after a successful load.

    Returns
    -------
    DAG
        Instantiated DAG object.
    """

    @dag(
        dag_id=f"load_pg_{dataset_config.name}",
        schedule=silver_file_asset,  # triggered by the ingestion DAG's outlet
        start_date=datetime(2026, 1, 24),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["load", "Postgres", "silver"],
        doc_md=__doc__,
    )
    def _dag() -> None:
        @task(
            task_id=TASK_LOAD,
            execution_timeout=TASK_LOAD_TIMEOUT,
            outlets=[silver_pg_asset],
        )
        def load_task() -> Generator[Metadata]:
            """Load silver parquet into the Postgres silver schema.

            Uses an Airflow ``PostgresHook`` (psycopg3) — credentials come
            from the Airflow connection store, connection lifecycle is managed
            inside ``load_silver_to_postgres_from_hook``.
            """
            # Lazy import: airflow providers only available inside the container.
            from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: PLC0415

            hook = PostgresHook("project_postgres")
            metrics = load_silver_to_postgres_from_hook(dataset_config=dataset_config, hook=hook)

            yield Metadata(
                asset=silver_pg_asset,
                extra={
                    "rows_loaded": metrics.rows_loaded,
                    "strategy": metrics.strategy,
                    "table": metrics.table,
                },
            )

        load_task()

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """Generate load DAGs for all remote datasets in the catalog.

    Returns
    -------
    dict[str, DAG]
        Mapping of dataset names to their load DAG objects.
    """
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.exception)
        return {}

    dags: dict[str, DAG] = {}

    for dataset_config in catalog.get_remote_datasets():
        try:
            silver_file_asset = get_silver_file_asset(dataset_config.name)
            silver_pg_asset = get_silver_pg_asset(dataset_config.name)
            dags[dataset_config.name] = _create_dag(
                dataset_config, silver_file_asset, silver_pg_asset
            )
            logger.info("Created load DAG", dag_id=f"load_pg_{dataset_config.name}")
        except ValueError:
            logger.exception("Failed to create load DAG", dataset_name=dataset_config.name)

    return dags


# Note: expose DAGs to Airflow
_generate_all_dags()
