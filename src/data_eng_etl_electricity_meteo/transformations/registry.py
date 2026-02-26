"""Explicit registry of dataset transformations.

Each dataset must register its bronze and silver transform functions here.
This is the single source of truth for available transformations — adding
a dataset to the catalog without registering its transforms will fail fast
in ``RemoteIngestionPipeline.__post_init__``.

Gold transformations are handled by dbt in Postgres, not in Python.
"""

from collections.abc import Callable
from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.enums import MedallionLayer
from data_eng_etl_electricity_meteo.core.exceptions import TransformNotFoundError
from data_eng_etl_electricity_meteo.transformations.shared import apply_common_silver

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

BronzeTransformFunc = Callable[[Path], pl.DataFrame]
SilverTransformFunc = Callable[[Path], pl.DataFrame]

# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

from data_eng_etl_electricity_meteo.transformations import (  # noqa: E402
    ign_contours_iris,
    meteo_france_stations,
    odre_eco2mix_cons_def,
    odre_eco2mix_tr,
    odre_installations,
)

BRONZE_TRANSFORMS: dict[str, BronzeTransformFunc] = {
    "ign_contours_iris": ign_contours_iris.transform_bronze,
    "meteo_france_stations": meteo_france_stations.transform_bronze,
    "odre_eco2mix_cons_def": odre_eco2mix_cons_def.transform_bronze,
    "odre_eco2mix_tr": odre_eco2mix_tr.transform_bronze,
    "odre_installations": odre_installations.transform_bronze,
}

SILVER_TRANSFORMS: dict[str, SilverTransformFunc] = {
    "ign_contours_iris": ign_contours_iris.transform_silver,
    "meteo_france_stations": meteo_france_stations.transform_silver,
    "odre_eco2mix_cons_def": odre_eco2mix_cons_def.transform_silver,
    "odre_eco2mix_tr": odre_eco2mix_tr.transform_silver,
    "odre_installations": odre_installations.transform_silver,
}

# ---------------------------------------------------------------------------
# Lookup functions
# ---------------------------------------------------------------------------


def get_bronze_transform(dataset_name: str) -> BronzeTransformFunc:
    """Retrieve the bronze transform for a dataset.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (must match a key in ``BRONZE_TRANSFORMS``).

    Returns
    -------
    BronzeTransformFunc
        Transform function: landing file path → bronze DataFrame.

    Raises
    ------
    TransformNotFoundError
        If no bronze transform is registered for *dataset_name*.
    """
    try:
        return BRONZE_TRANSFORMS[dataset_name]
    except KeyError:
        raise TransformNotFoundError(
            dataset_name=dataset_name, layer=MedallionLayer.BRONZE
        ) from None


def get_silver_transform(dataset_name: str) -> SilverTransformFunc:
    """Retrieve the silver transform for a dataset.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (must match a key in ``SILVER_TRANSFORMS``).

    Returns
    -------
    SilverTransformFunc
        Transform function: bronze file path → silver DataFrame.

    Raises
    ------
    TransformNotFoundError
        If no silver transform is registered for *dataset_name*.
    """
    try:
        specific_fn = SILVER_TRANSFORMS[dataset_name]
    except KeyError:
        raise TransformNotFoundError(
            dataset_name=dataset_name, layer=MedallionLayer.SILVER
        ) from None

    def wrapped(path: Path) -> pl.DataFrame:
        df = specific_fn(path)
        return apply_common_silver(df, dataset_name)

    return wrapped
