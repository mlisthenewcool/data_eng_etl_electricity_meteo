"""Unit tests for DatasetTransformSpec — conditional dedup in run_silver."""

from pathlib import Path

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.transformations.dataframe_model import DataFrameModel
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

# --------------------------------------------------------------------------------------
# Minimal schema for tests
# --------------------------------------------------------------------------------------


class _MinimalSchema(DataFrameModel):
    key: str
    val: int


def _identity_bronze(landing_path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(landing_path)


def _identity_silver(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf


def _make_spec(*, primary_key: tuple[str, ...] = ("key",)) -> DatasetTransformSpec:
    return DatasetTransformSpec(
        name="test_dataset",
        bronze_transform=_identity_bronze,
        silver_transform=_identity_silver,
        primary_key=primary_key,
        all_source_columns=frozenset({"key", "val"}),
        used_source_columns=frozenset({"key", "val"}),
        silver_schema=_MinimalSchema,
    )


# --------------------------------------------------------------------------------------
# run_silver conditional dedup
# --------------------------------------------------------------------------------------


class TestRunSilverDedup:
    def test_dedup_applied_when_duplicates(self, tmp_path: Path) -> None:
        """Duplicate rows on primary key are removed (keep last)."""
        df = pl.DataFrame({"key": ["a", "a", "b"], "val": [1, 2, 3]})
        bronze_path = tmp_path / "bronze.parquet"
        df.write_parquet(bronze_path)
        spec = _make_spec()

        result = spec.run_silver(bronze_path)

        assert len(result) == 2
        # "keep last" → val=2 for key "a"
        row_a = result.filter(pl.col("key") == "a")
        assert row_a["val"].item() == 2

    def test_dedup_skipped_when_no_duplicates(self, tmp_path: Path) -> None:
        """No duplicates → DataFrame passes through unchanged."""
        df = pl.DataFrame({"key": ["a", "b", "c"], "val": [1, 2, 3]})
        bronze_path = tmp_path / "bronze.parquet"
        df.write_parquet(bronze_path)
        spec = _make_spec()

        result = spec.run_silver(bronze_path)

        assert len(result) == 3

    def test_dedup_logs_removal_count(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Dedup should log the number of removed rows."""
        df = pl.DataFrame({"key": ["a", "a", "a", "b"], "val": [1, 2, 3, 4]})
        bronze_path = tmp_path / "bronze.parquet"
        df.write_parquet(bronze_path)
        spec = _make_spec()

        result = spec.run_silver(bronze_path)

        assert len(result) == 2
