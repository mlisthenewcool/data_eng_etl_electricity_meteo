"""Factory for Airflow Asset objects tied to medallion layers.

Airflow Assets represent data dependencies between DAGs / tasks.
Each output layer (silver, gold) produces an Asset that downstream
DAGs can declare as an inlet to trigger on update.

Assets are identified by URI. This module guarantees that repeated
calls with the same arguments return the **same** ``Asset`` instance
(via ``@cache``), avoiding redundant object creation.
"""

from functools import cache

from airflow.sdk import Asset

from data_eng_etl_electricity_meteo.core.layers import MedallionLayer
from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
    DerivedPathResolver,
    RemotePathResolver,
)

__all__: list[str] = ["get_asset"]


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
    if layer == "silver":
        resolver = RemotePathResolver(dataset_name)
        uri = f"file:///{resolver.silver_current_path}"
    elif layer == "gold":
        resolver = DerivedPathResolver(dataset_name)
        uri = f"file:///{resolver.gold_current_path}"
    else:
        raise ValueError(f"No Airflow Asset for layer {layer!r} (only 'silver' and 'gold').")

    return Asset(
        name=f"{dataset_name}__{layer}",
        uri=uri,
        group=layer,
        extra={"dataset_name": dataset_name},
    )
