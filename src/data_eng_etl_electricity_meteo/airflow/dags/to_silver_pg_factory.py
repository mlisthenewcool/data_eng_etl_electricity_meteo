"""DAG factory for silver Postgres loading (silver file → silver PG).

Generates one ``{dataset}_to_silver_pg`` DAG per remote dataset in the data catalog.
Each DAG is triggered when its upstream ``to_silver`` DAG produces a new silver file
Asset, and loads the Parquet into Postgres ``silver.{dataset}`` via ``PostgresHook``
(connection id defined in ``pg_connection.AIRFLOW_CONN_ID``).
"""

from collections.abc import Generator
from contextlib import closing
from datetime import timedelta

from airflow.sdk import DAG, Asset, Metadata, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_silver_file_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import InvalidCatalogError
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.pg_connection import open_airflow_connection
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
    *,
    schedule: Asset,
    outlet: Asset,
) -> DAG:
    """Create a load DAG for a single dataset.

    Parameters
    ----------
    dataset
        Remote dataset configuration from the catalog.
    schedule
        The silver ``file://`` Asset that triggers this DAG.
    outlet
        The silver Postgres Asset emitted after a successful load.

    Returns
    -------
    DAG
        Instantiated DAG object.
    """

    @dag(
        dag_id=f"{dataset.name}_to_silver_pg",
        schedule=schedule,
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
            outlets=[outlet],
        )
        def load_task() -> Generator[Metadata]:
            """Load silver Parquet into the Postgres silver schema.

            Uses an Airflow ``PostgresHook`` (psycopg3) — credentials come from the
            Airflow connection store.
            """
            # closing() ensures close-without-commit (unlike `with conn:`
            # which auto-commits on success).
            with closing(open_airflow_connection()) as conn:
                metrics = load_silver_to_postgres(dataset, conn=conn)

            yield Metadata(
                asset=outlet,
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
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    remote_datasets = catalog.get_remote_datasets()
    dags: dict[str, DAG] = {}

    for dataset in remote_datasets:
        try:
            silver_file_asset = get_silver_file_asset(dataset)
            silver_pg_asset = get_silver_pg_asset(dataset)
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset_name=dataset.name)
            continue  # move to next dataset

        dags[dataset.name] = _create_dag(
            dataset, schedule=silver_file_asset, outlet=silver_pg_asset
        )
        logger.debug("to_silver_pg DAG created", dataset_name=dataset.name)

    logger.info(
        "to_silver_pg factory completed", created_count=len(dags), total_count=len(remote_datasets)
    )

    return dags


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
