"""Unit tests for the core data catalog module."""

from datetime import datetime
from pathlib import Path

import pytest

from data_eng_etl_electricity_meteo.core.data_catalog import (
    DataCatalog,
    IngestionFrequency,
    SourceFormat,
    _dataset_discriminator,
)
from data_eng_etl_electricity_meteo.core.exceptions import (
    DatasetNotFoundError,
    DatasetTypeError,
    InvalidCatalogError,
)

_SAMPLE_CATALOG = Path(__file__).parent.parent / "fixtures" / "data_catalog_sample.yaml"


@pytest.fixture
def catalog() -> DataCatalog:
    return DataCatalog.load(_SAMPLE_CATALOG)


# ---------------------------------------------------------------------------
# SourceFormat
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    argnames="fmt, expected",
    argvalues=[
        (SourceFormat.SEVEN_Z, True),
        (SourceFormat.PARQUET, False),
        (SourceFormat.JSON, False),
    ],
)
def test_source_format_is_archive(fmt: SourceFormat, expected: bool) -> None:
    assert fmt.is_archive is expected


# ---------------------------------------------------------------------------
# IngestionFrequency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    argnames="freq, expected",
    argvalues=[
        (IngestionFrequency.HOURLY, "@hourly"),
        (IngestionFrequency.DAILY, "@daily"),
        (IngestionFrequency.WEEKLY, "@weekly"),
        (IngestionFrequency.MONTHLY, "@monthly"),
        (IngestionFrequency.YEARLY, "@yearly"),
        (IngestionFrequency.NEVER, None),
    ],
)
def test_ingestion_frequency_airflow_schedule(
    freq: IngestionFrequency, expected: str | None
) -> None:
    assert freq.airflow_schedule == expected


class TestIngestionFrequencyFormatDatetimeAsVersion:
    _DT = datetime(year=2024, month=3, day=15, hour=10, minute=30, second=0)
    _F = IngestionFrequency

    # fmt: off
    @pytest.mark.parametrize(
        argnames="freq, no_dash, expected",
        argvalues=[
            pytest.param(_F.HOURLY,  True,  "20240315T103000",       id="hourly_no_dash"),
            pytest.param(_F.HOURLY,  False, "2024-03-15-T-10-30-00", id="hourly_with_dash"),
            pytest.param(_F.DAILY,   True,  "20240315",              id="daily_no_dash"),
            pytest.param(_F.DAILY,   False, "2024-03-15",            id="daily_with_dash"),
            pytest.param(_F.WEEKLY,  True,  "20240315",              id="non_daily_date_only"),
        ],
    )
    # fmt: on
    def test_format_datetime_as_version(
        self, freq: IngestionFrequency, no_dash: bool, expected: str
    ) -> None:
        assert freq.format_datetime_as_version(self._DT, no_dash=no_dash) == expected


# ---------------------------------------------------------------------------
# _dataset_discriminator
# ---------------------------------------------------------------------------
class TestDatasetDiscriminator:
    """Test the dict branches of the discriminator (our routing logic).

    Instance branches are not tested here: they only check isinstance(), which
    is a pydantic concern, not ours.
    """

    def test_dict_with_url_in_source_is_remote(self) -> None:
        v: dict[str, object] = {
            "source": {"url": "https://example.com/data.parquet", "provider": "RTE"}
        }
        assert _dataset_discriminator(v) == "remote"

    def test_dict_without_url_is_gold(self) -> None:
        v: dict[str, object] = {"source": {"depends_on": ["other"]}}
        assert _dataset_discriminator(v) == "derived"


# ---------------------------------------------------------------------------
# DataCatalog.load — error handling and name injection
# ---------------------------------------------------------------------------
class TestDataCatalogLoad:
    def test_missing_file_raises_invalid_catalog_error(self, tmp_path: Path) -> None:
        with pytest.raises(InvalidCatalogError):
            DataCatalog.load(tmp_path / "nonexistent.yaml")

    def test_empty_file_raises_invalid_catalog_error(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.yaml"
        path.write_text("")
        with pytest.raises(InvalidCatalogError):
            DataCatalog.load(path)

    def test_invalid_yaml_syntax_raises_invalid_catalog_error(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.yaml"
        path.write_text("datasets: {unclosed: [}")
        with pytest.raises(InvalidCatalogError):
            DataCatalog.load(path)

    def test_name_injected_from_yaml_key(self, catalog: DataCatalog) -> None:
        assert catalog.datasets["electricity_mix"].name == "electricity_mix"
        assert catalog.datasets["energy_weather"].name == "energy_weather"


# ---------------------------------------------------------------------------
# DataCatalog.get_dataset
# ---------------------------------------------------------------------------
class TestDataCatalogGetDataset:
    def test_raises_dataset_not_found_error(self, catalog: DataCatalog) -> None:
        with pytest.raises(DatasetNotFoundError):
            catalog.get_remote_dataset("unknown_dataset")

    def test_get_remote_dataset_raises_type_error_for_gold(self, catalog: DataCatalog) -> None:
        with pytest.raises(DatasetTypeError):
            catalog.get_remote_dataset("energy_weather")

    def test_get_gold_dataset_raises_type_error_for_remote(self, catalog: DataCatalog) -> None:
        with pytest.raises(DatasetTypeError):
            catalog.get_gold_dataset("electricity_mix")
