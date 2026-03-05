"""Validate that every remote dataset in the catalog has a transform spec."""

import pytest

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.transformations.registry import get_transform_spec

_catalog = DataCatalog.load(settings.data_catalog_file_path)
_remote_datasets = _catalog.get_remote_datasets()


@pytest.mark.parametrize("dataset", _remote_datasets, ids=lambda d: d.name)
def test_remote_dataset_has_transform_spec(dataset: RemoteDatasetConfig) -> None:
    """Ensure a transform spec is registered for this dataset."""
    spec = get_transform_spec(dataset.name)
    assert len(spec.all_source_columns) > 0
