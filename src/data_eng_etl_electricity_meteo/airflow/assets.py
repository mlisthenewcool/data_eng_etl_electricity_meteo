"""Factory for Airflow Asset objects tied to medallion layers.

Airflow Assets represent data dependencies between DAGs / tasks.
Each output layer (silver, gold) produces an Asset that downstream
DAGs can declare as an inlet to trigger on update.

Two Asset families exist:

- **File Assets** (``get_asset``): ``file://`` URIs pointing to Parquet files
  on disk. Produced by ingestion DAGs (silver) and gold compute DAGs.
- **PostgreSQL Assets** (``get_silver_pg_asset``): ``postgres://`` URIs
  representing tables in the project database. Produced by load DAGs and
  consumed by the dbt transformation DAG.

Assets are identified by URI. This module guarantees that repeated
calls with the same arguments return the **same** ``Asset`` instance
(via ``@cache``), avoiding redundant object creation.
"""

from functools import cache

from airflow.sdk import Asset

from data_eng_etl_electricity_meteo.core.layers import MedallionLayer
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
    DerivedPathResolver,
    RemotePathResolver,
)


@cache
def get_asset(dataset_name: str, layer: MedallionLayer) -> Asset:
    """Build an Airflow Asset for a dataset at a given medallion layer.

    Only ``"silver"`` and ``"gold"`` layers produce Assets, since
    landing and bronze are internal pipeline stages with no
    downstream consumers.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (must match a catalog key).
    layer:
        Target medallion layer.

    Returns
    -------
    Asset
        Airflow Asset with a ``file://`` URI pointing to the
        layer's ``current.parquet``.

    Raises
    ------
    ValueError
        If *layer* is not ``"silver"`` or ``"gold"``.
    """
    if layer == MedallionLayer.SILVER:
        resolver = RemotePathResolver(dataset_name)
        uri = resolver.silver_current_path.as_uri()
    elif layer == MedallionLayer.GOLD:
        resolver = DerivedPathResolver(dataset_name)
        uri = resolver.gold_current_path.as_uri()
    else:
        raise ValueError(f"No Airflow Asset for layer {layer!r} (only 'silver' and 'gold').")

    return Asset(
        name=f"{dataset_name}__{layer}",
        uri=uri,
        group=layer,
        extra={"dataset_name": dataset_name},
    )


@cache
def get_silver_pg_asset(dataset_name: str) -> Asset:
    """Build an Airflow Asset for a dataset's PostgreSQL silver table.

    These Assets represent data loaded into the ``silver`` schema of the
    project PostgreSQL database. They serve as outlets for load DAGs
    and as inlets for the dbt transformation DAG.

    The URI is a logical marker — Airflow does not inspect the database;
    the outlet event is emitted by the load task upon successful completion.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (must match a catalog key).

    Returns
    -------
    Asset
        Airflow Asset with a ``postgres://`` URI identifying the silver table.
    """
    # URI format required by apache-airflow-providers-postgres: postgres://host:port/db/schema/table
    uri = (
        f"postgres://{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.project_db_name}/silver/{dataset_name}"
    )
    return Asset(
        name=f"{dataset_name}__silver_pg",
        uri=uri,
        group="silver_pg",
        extra={"dataset_name": dataset_name},
    )
