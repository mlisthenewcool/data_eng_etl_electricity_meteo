"""Factory for Airflow Asset objects tied to medallion layers.

Airflow Assets represent data dependencies between DAGs / tasks.

Two Asset families exist:

- **File Assets** (``get_silver_file_asset``): ``file://`` URIs pointing to silver
  Parquet files on disk. Produced by ingestion DAGs.
- **Postgres Assets** (``get_silver_pg_asset``): ``postgres://`` URIs representing
  tables in the project database.
  Produced by load DAGs and consumed by the dbt transformation DAG.

Assets are identified by URI.
This module guarantees that repeated calls with the same arguments return the **same**
``Asset`` instance (via ``@cache``), avoiding redundant object creation.
"""

from functools import cache

from airflow.sdk import Asset

from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver

# Asset group constants — these describe storage targets, not data layers
# (intentionally not in MedallionLayer which is a data-layer concept).
SILVER_FILE_GROUP = "silver"
SILVER_PG_GROUP = "silver_pg"
GOLD_PG_GROUP = "gold_pg"

# TODO: add meaningful extras to Assets or remove them ?


@cache
def get_silver_file_asset(dataset_name: str) -> Asset:
    """Build an Airflow Asset for a dataset's silver Parquet file.

    Landing and bronze are internal pipeline stages with no downstream consumers.
    Gold datasets live in Postgres (via dbt).

    Parameters
    ----------
    dataset_name
        Dataset identifier (must match a catalog key).

    Returns
    -------
    Asset
        Airflow Asset with a ``file://`` URI pointing to
        ``silver/{dataset_name}/current.parquet``.
    """
    resolver = RemotePathResolver(dataset_name)
    uri = resolver.silver_current_path.as_uri()

    return Asset(
        name=f"{dataset_name}__{SILVER_FILE_GROUP}",
        uri=uri,
        group=SILVER_FILE_GROUP,
        extra={"dataset_name": dataset_name},
    )


@cache
def get_silver_pg_asset(dataset_name: str) -> Asset:
    """Build an Airflow Asset for a dataset's Postgres silver table.

    These Assets represent data loaded into the ``silver`` schema of the project
    Postgres database.
    They serve as outlets for load DAGs and as inlets for the dbt transformation DAG.

    The URI is a logical marker — Airflow does not inspect the database; the outlet
    event is emitted by the load task upon successful completion.

    Parameters
    ----------
    dataset_name
        Dataset identifier (must match a catalog key).

    Returns
    -------
    Asset
        Airflow Asset with a ``postgres://`` URI identifying the silver table.
    """
    # URI format required by apache-airflow-providers-postgres: postgres://host:port/db/schema/table
    uri = (
        f"postgres://{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.postgres_db_name}/silver/{dataset_name}"
    )
    return Asset(
        name=f"{dataset_name}__{SILVER_PG_GROUP}",
        uri=uri,
        group=SILVER_PG_GROUP,
        extra={"dataset_name": dataset_name},
    )


@cache
def get_gold_pg_asset(dataset_name: str) -> Asset:
    """Build an Airflow Asset for a dataset's Postgres gold table.

    These Assets represent data produced by dbt in the ``gold`` schema of the project
    Postgres database. They serve as outlets for the dbt DAG.

    Parameters
    ----------
    dataset_name
        Dataset identifier (must match a catalog key).

    Returns
    -------
    Asset
        Airflow Asset with a ``postgres://`` URI identifying the gold table.
    """
    uri = (
        f"postgres://{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.postgres_db_name}/gold/{dataset_name}"
    )
    return Asset(
        name=f"{dataset_name}__{GOLD_PG_GROUP}",
        uri=uri,
        group=GOLD_PG_GROUP,
        extra={"dataset_name": dataset_name},
    )
