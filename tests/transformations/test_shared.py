"""Unit tests for shared transformation utilities (diagnostics, guard)."""

from unittest.mock import patch

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.transformations.shared import (
    DIAG_PREFIX,
    WARN_PREFIX,
    extract_diagnostics,
    prepare_silver,
)

_LOGGER_PATH = "data_eng_etl_electricity_meteo.transformations.shared.logger"

# --------------------------------------------------------------------------------------
# extract_diagnostics
# --------------------------------------------------------------------------------------


class TestExtractDiagnostics:
    def test_drops_warn_and_diag_columns(self) -> None:
        df = pl.DataFrame(
            {
                "a": [1],
                "_warn_bad_rows": [5],
                "_diag_total_rows": [100],
            }
        )
        result = extract_diagnostics(df)
        assert result.columns == ["a"]

    def test_nonzero_warn_logs_warning(self) -> None:
        df = pl.DataFrame({"a": [1], "_warn_cast_nulls_x": [3]})
        with patch(_LOGGER_PATH) as mock_logger:
            extract_diagnostics(df)
        mock_logger.warning.assert_called_once_with(
            "Data quality issue", metric="cast_nulls_x", count=3
        )

    def test_zero_warn_does_not_log(self) -> None:
        df = pl.DataFrame({"a": [1], "_warn_cast_nulls_x": [0]})
        with patch(_LOGGER_PATH) as mock_logger:
            extract_diagnostics(df)
        mock_logger.warning.assert_not_called()

    def test_nonzero_diag_logs_info(self) -> None:
        df = pl.DataFrame({"a": [1], "_diag_duplicate_rows_removed": [42]})
        with patch(_LOGGER_PATH) as mock_logger:
            extract_diagnostics(df)
        mock_logger.info.assert_called_once_with("Silver diagnostics", duplicate_rows_removed=42)

    def test_zero_diag_does_not_log(self) -> None:
        df = pl.DataFrame({"a": [1], "_diag_duplicate_rows_removed": [0]})
        with patch(_LOGGER_PATH) as mock_logger:
            extract_diagnostics(df)
        mock_logger.info.assert_not_called()

    def test_returns_data_unchanged(self) -> None:
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"], "_diag_foo": [10, 10]})
        result = extract_diagnostics(df)
        assert result.columns == ["a", "b"]
        assert result["a"].to_list() == [1, 2]

    def test_mixed_warn_and_diag(self) -> None:
        df = pl.DataFrame(
            {
                "a": [1],
                "_warn_x": [5],
                "_warn_y": [0],
                "_diag_z": [10],
            }
        )
        with patch(_LOGGER_PATH) as mock_logger:
            result = extract_diagnostics(df)
        assert result.columns == ["a"]
        mock_logger.warning.assert_called_once_with("Data quality issue", metric="x", count=5)
        mock_logger.info.assert_called_once_with("Silver diagnostics", z=10)


# --------------------------------------------------------------------------------------
# prepare_silver guard
# --------------------------------------------------------------------------------------


class TestPrepareSilverGuard:
    def test_raises_on_diag_prefix_column(self) -> None:
        lf = pl.DataFrame({"col_a": [1], f"{DIAG_PREFIX}bad": [2]}).lazy()
        with pytest.raises(ValueError, match="reserved diagnostic prefix"):
            prepare_silver(lf, dataset_name="test")

    def test_raises_on_warn_prefix_column(self) -> None:
        lf = pl.DataFrame({"col_a": [1], f"{WARN_PREFIX}bad": [2]}).lazy()
        with pytest.raises(ValueError, match="reserved diagnostic prefix"):
            prepare_silver(lf, dataset_name="test")

    def test_passes_for_normal_columns(self) -> None:
        lf = pl.DataFrame({"ColA": [1], "ColB": [2]}).lazy()
        result = prepare_silver(lf, dataset_name="test")
        assert set(result.collect_schema().names()) == {"col_a", "col_b"}
