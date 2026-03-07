"""DAG factory for silver Postgres loading (silver file → silver PG).

Generates one ``{dataset}_to_silver_pg`` DAG per remote dataset in the data catalog.
Each DAG is triggered when its upstream ``to_silver`` DAG produces a new silver file
Asset, and loads the parquet into Postgres ``silver.{dataset}`` via ``PostgresHook``
(connection id ``project_postgres``).
"""

from collections.abc import Generator
from datetime import timedelta

from airflow.sdk import DAG, Asset, Metadata, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_silver_file_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import InvalidCatalogError
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.pg_connection import open_airflow_hook_connection
from data_eng_etl_electricity_meteo.loaders.pg_loader import load_silver_to_postgres

logger = get_logger("dag.to_silver_pg")


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


TASK_LOAD = "load_silver_to_postgres"
TASK_LOAD_TIMEOUT = timedelta(minutes=20)


# --------------------------------------------------------------------------------------
# DAG factory
# --------------------------------------------------------------------------------------


def _create_dag(
    dataset: RemoteDatasetConfig,
    silver_file_asset: Asset,
    silver_pg_asset: Asset,
) -> DAG:
    """Create a load DAG for a single dataset.

    Parameters
    ----------
    dataset
        Remote dataset configuration from the catalog.
    silver_file_asset
        Inlet: the silver ``file://`` Asset produced by the ingestion DAG.
    silver_pg_asset
        Outlet: the silver Postgres Asset emitted after a successful load.

    Returns
    -------
    DAG
        Instantiated DAG object.
    """

    @dag(
        dag_id=f"{dataset.name}_to_silver_pg",
        schedule=silver_file_asset,  # triggered by the ingestion DAG's outlet
        start_date=START_DATE,
        catchup=False,  # TODO[prod]: set to True
        default_args=DEFAULT_ARGS,
        tags=["load", "postgres", "silver"],
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

            Uses an Airflow ``PostgresHook`` (psycopg3) — credentials come from the
            Airflow connection store.
            """
            # Lazy import: airflow providers only available inside the container.
            from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: PLC0415

            conn = open_airflow_hook_connection(PostgresHook("project_postgres"))
            try:
                metrics = load_silver_to_postgres(dataset=dataset, conn=conn)
            finally:
                conn.close()

            yield Metadata(
                asset=silver_pg_asset,
                extra={
                    "rows_loaded": metrics.rows_loaded,
                    "mode": metrics.mode,
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
        catalog = DataCatalog.load(path=settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    dags: dict[str, DAG] = {}

    for dataset in catalog.get_remote_datasets():
        try:
            silver_file_asset = get_silver_file_asset(dataset)
            silver_pg_asset = get_silver_pg_asset(dataset)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset=dataset.name)
            continue  # move to next dataset

        dags[dataset.name] = _create_dag(dataset, silver_file_asset, silver_pg_asset)
        logger.info("to_silver_pg DAG created", dataset=dataset.name)

    total = len(catalog.get_remote_datasets())
    logger.info("to_silver_pg factory complete", created_count=len(dags), total_count=total)

    return dags


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
