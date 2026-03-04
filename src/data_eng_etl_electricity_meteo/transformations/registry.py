"""Explicit registry of dataset transformations.

Each dataset must register its bronze and silver transform functions here.
This is the single source of truth for available transformations — adding a dataset to
the catalog without registering its transforms will fail fast in
``RemoteIngestionPipeline.__post_init__``.

Gold transformations are handled by dbt in Postgres, not in Python.
"""

from collections.abc import Callable
from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.enums import MedallionLayer
from data_eng_etl_electricity_meteo.core.exceptions import TransformNotFoundError
from data_eng_etl_electricity_meteo.transformations.shared import (
    prepare_silver,
    validate_no_nulls,
    validate_not_empty,
    validate_unique,
)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

BronzeTransformFunc = Callable[[Path], pl.LazyFrame]

# Silver transforms receive a pre-processed DataFrame (snake_case columns,
# all-null columns dropped) and return the transformed DataFrame.
SilverTransformFunc = Callable[[pl.DataFrame], pl.DataFrame]

# External signature used by RemoteIngestionPipeline (unchanged).
WrappedSilverTransformFunc = Callable[[Path], pl.DataFrame]

# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

from data_eng_etl_electricity_meteo.transformations import (  # noqa: E402
    ign_contours_iris,
    meteo_france_climatologie,
    meteo_france_stations,
    odre_eco2mix_cons_def,
    odre_eco2mix_tr,
    odre_installations,
)

BRONZE_TRANSFORMS: dict[str, BronzeTransformFunc] = {
    "ign_contours_iris": ign_contours_iris.transform_bronze,
    "meteo_france_climatologie": meteo_france_climatologie.transform_bronze,
    "meteo_france_stations": meteo_france_stations.transform_bronze,
    "odre_eco2mix_cons_def": odre_eco2mix_cons_def.transform_bronze,
    "odre_eco2mix_tr": odre_eco2mix_tr.transform_bronze,
    "odre_installations": odre_installations.transform_bronze,
}

PrimaryKey = str | list[str]

SILVER_TRANSFORMS: dict[str, tuple[SilverTransformFunc, PrimaryKey]] = {
    "ign_contours_iris": (ign_contours_iris.transform_silver, "code_iris"),
    "meteo_france_climatologie": (
        meteo_france_climatologie.transform_silver,
        ["id_station", "date_heure"],
    ),
    "meteo_france_stations": (meteo_france_stations.transform_silver, "id"),
    "odre_eco2mix_cons_def": (
        odre_eco2mix_cons_def.transform_silver,
        ["code_insee_region", "date_heure"],
    ),
    "odre_eco2mix_tr": (
        odre_eco2mix_tr.transform_silver,
        ["code_insee_region", "date_heure"],
    ),
    "odre_installations": (odre_installations.transform_silver, "id_peps"),
}

# ---------------------------------------------------------------------------
# Lookup functions
# ---------------------------------------------------------------------------


def get_bronze_transform(dataset_name: str) -> BronzeTransformFunc:
    """Retrieve the bronze transform for a dataset.

    Parameters
    ----------
    dataset_name
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


def get_silver_transform(dataset_name: str) -> WrappedSilverTransformFunc:
    """Retrieve the silver transform for a dataset, wrapped with common steps.

    The returned function reads the bronze parquet, applies common pre-processing
    (snake_case rename, drop all-null columns), passes the prepared DataFrame to the
    dataset-specific transform, and validates the result is not empty.

    Parameters
    ----------
    dataset_name
        Dataset identifier (must match a key in ``SILVER_TRANSFORMS``).

    Returns
    -------
    WrappedSilverTransformFunc
        Wrapped transform: bronze file path → silver DataFrame.

    Raises
    ------
    TransformNotFoundError
        If no silver transform is registered for *dataset_name*.
    """
    try:
        specific_fn, primary_key = SILVER_TRANSFORMS[dataset_name]
    except KeyError:
        raise TransformNotFoundError(
            dataset_name=dataset_name, layer=MedallionLayer.SILVER
        ) from None

    def wrapped(path: Path) -> pl.DataFrame:
        df = pl.read_parquet(path)
        df = prepare_silver(df, dataset_name)
        df = specific_fn(df)
        validate_not_empty(df, dataset_name)
        if primary_key:
            validate_no_nulls(df, primary_key, dataset_name)
            validate_unique(df, primary_key, dataset_name)
        return df

    return wrapped
