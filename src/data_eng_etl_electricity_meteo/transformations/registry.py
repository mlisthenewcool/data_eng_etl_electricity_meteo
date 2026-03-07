"""Explicit registry of dataset transformations.

Each dataset module defines a ``SPEC`` — an immutable ``DatasetTransformSpec`` that
bundles its bronze/silver transforms, source column sets, and silver schema.
This module collects those specs into a private registry dict.

Adding a dataset to the catalog without registering its spec will fail fast in
``RemoteIngestionPipeline.__post_init__``.

Gold transformations are handled by dbt in Postgres, not in Python.
"""

from data_eng_etl_electricity_meteo.core.exceptions import TransformNotFoundError
from data_eng_etl_electricity_meteo.transformations import (
    ign_contours_iris,
    meteo_france_climatologie,
    meteo_france_stations,
    odre_eco2mix_cons_def,
    odre_eco2mix_tr,
    odre_installations,
)
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

# --------------------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------------------


_REGISTRY: dict[str, DatasetTransformSpec] = {
    spec.name: spec
    for spec in (
        ign_contours_iris.SPEC,
        meteo_france_climatologie.SPEC,
        meteo_france_stations.SPEC,
        odre_eco2mix_cons_def.SPEC,
        odre_eco2mix_tr.SPEC,
        odre_installations.SPEC,
    )
}


def get_transform_spec(dataset_name: str) -> DatasetTransformSpec:
    """Retrieve the transform spec for a dataset.

    Parameters
    ----------
    dataset_name
        Dataset identifier (must match a key in the registry).

    Returns
    -------
    DatasetTransformSpec
        The registered transform spec.

    Raises
    ------
    TransformNotFoundError
        If no spec is registered for *dataset_name*.
    """
    try:
        return _REGISTRY[dataset_name]
    except KeyError:
        raise TransformNotFoundError(dataset_name=dataset_name) from None
