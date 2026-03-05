"""Unit tests for validate_source_columns (source drift detection)."""

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.core.exceptions import SourceSchemaDriftError
from data_eng_etl_electricity_meteo.transformations.shared import validate_source_columns

_EXPECTED = {"col_a", "col_b", "col_c"}
_DATASET = "test_dataset"


class TestValidateSourceColumns:
    def test_exact_match_passes(self) -> None:
        df = pl.DataFrame({"col_a": [1], "col_b": [2], "col_c": [3]})
        validate_source_columns(df, _EXPECTED, _DATASET)

    def test_added_columns_raises(self) -> None:
        df = pl.DataFrame({"col_a": [1], "col_b": [2], "col_c": [3], "col_new": [4]})
        with pytest.raises(SourceSchemaDriftError) as exc_info:
            validate_source_columns(df, _EXPECTED, _DATASET)
        assert exc_info.value.added == ["col_new"]
        assert exc_info.value.removed == []
        assert exc_info.value.dataset_name == _DATASET

    def test_removed_columns_raises(self) -> None:
        df = pl.DataFrame({"col_a": [1], "col_b": [2]})
        with pytest.raises(SourceSchemaDriftError) as exc_info:
            validate_source_columns(df, _EXPECTED, _DATASET)
        assert exc_info.value.added == []
        assert exc_info.value.removed == ["col_c"]

    def test_added_and_removed_columns_raises(self) -> None:
        df = pl.DataFrame({"col_a": [1], "col_x": [2], "col_y": [3]})
        with pytest.raises(SourceSchemaDriftError) as exc_info:
            validate_source_columns(df, _EXPECTED, _DATASET)
        assert exc_info.value.added == ["col_x", "col_y"]
        assert exc_info.value.removed == ["col_b", "col_c"]

    def test_column_order_does_not_matter(self) -> None:
        df = pl.DataFrame({"col_c": [1], "col_a": [2], "col_b": [3]})
        validate_source_columns(df, _EXPECTED, _DATASET)
