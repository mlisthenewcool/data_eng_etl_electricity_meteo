from typing import Literal

from airflow.sdk import Asset


def get_asset(dataset_name: str, layer: Literal["silver", "gold"]) -> Asset:
    return Asset(
        name=f"{dataset_name}__{layer}",
        uri=f"file:///opt/airflow/data/{dataset_name}.parquet",
        group=layer,
        extra={"dataset_name": dataset_name},
    )


ASSETS = {
    "ign_contours_iris": get_asset("ign_contours_iris", layer="silver"),
}
