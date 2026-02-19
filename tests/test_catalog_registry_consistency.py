"""Validate that every remote dataset in the catalog has its transforms registered."""

import pytest

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.transformations.registry import (
    get_bronze_transform,
    get_silver_transform,
)

_catalog = DataCatalog.load(settings.data_catalog_file_path)
_remote_datasets = _catalog.get_remote_datasets()


@pytest.mark.parametrize("dataset", _remote_datasets, ids=lambda d: d.name)
def test_remote_dataset_has_bronze_transform(dataset):
    """Ensure a bronze transform is registered for this dataset."""
    get_bronze_transform(dataset.name)


@pytest.mark.parametrize("dataset", _remote_datasets, ids=lambda d: d.name)
def test_remote_dataset_has_silver_transform(dataset):
    """Ensure a silver transform is registered for this dataset."""
    get_silver_transform(dataset.name)
