"""Explicit registry of dataset transformations.

Each dataset must register its bronze, silver, and/or gold transform
functions here. This is the single source of truth for available
transformations — adding a dataset to the catalog without registering
its transforms will fail fast in ``RemoteDatasetPipeline.__post_init__``.
"""

from collections.abc import Callable
from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.exceptions import TransformNotFoundError

__all__: list[str] = [
    "BronzeTransformFunc",
    "SilverTransformFunc",
    "GoldTransformFunc",
    "get_bronze_transform",
    "get_silver_transform",
    "get_gold_transform",
]

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

type BronzeTransformFunc = Callable[[Path], pl.DataFrame]
type SilverTransformFunc = Callable[[Path], pl.DataFrame]
type GoldTransformFunc = Callable[..., pl.DataFrame]

# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

from data_eng_etl_electricity_meteo.transformations import ign_contours_iris  # noqa: E402

BRONZE_TRANSFORMS: dict[str, BronzeTransformFunc] = {
    "ign_contours_iris": ign_contours_iris.transform_bronze,
}

SILVER_TRANSFORMS: dict[str, SilverTransformFunc] = {
    "ign_contours_iris": ign_contours_iris.transform_silver,
}

GOLD_TRANSFORMS: dict[str, GoldTransformFunc] = {}

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
        raise TransformNotFoundError(dataset_name=dataset_name, layer="bronze") from None


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
        return SILVER_TRANSFORMS[dataset_name]
    except KeyError:
        raise TransformNotFoundError(dataset_name=dataset_name, layer="silver") from None


def get_gold_transform(dataset_name: str) -> GoldTransformFunc:
    """Retrieve the gold transform for a dataset.

    Parameters
    ----------
    dataset_name:
        Dataset identifier (must match a key in ``GOLD_TRANSFORMS``).

    Returns
    -------
    GoldTransformFunc
        Transform function: silver sources → gold DataFrame.

    Raises
    ------
    TransformNotFoundError
        If no gold transform is registered for *dataset_name*.
    """
    try:
        return GOLD_TRANSFORMS[dataset_name]
    except KeyError:
        raise TransformNotFoundError(dataset_name=dataset_name, layer="gold") from None
