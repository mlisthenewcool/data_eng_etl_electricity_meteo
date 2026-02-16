"""Airflow Asset definitions for the medallion pipeline."""

from typing import Literal

from airflow.sdk import Asset

__all__: list[str] = ["get_asset", "ASSETS"]


def get_asset(dataset_name: str, layer: Literal["silver", "gold"]) -> Asset:
    """Build an Airflow Asset for a dataset at a given layer."""
    return Asset(
        name=f"{dataset_name}__{layer}",
        uri=f"file:///opt/airflow/data/{dataset_name}.parquet",
        group=layer,
        extra={"dataset_name": dataset_name},
    )


ASSETS = {
    "ign_contours_iris": get_asset("ign_contours_iris", layer="silver"),
}
