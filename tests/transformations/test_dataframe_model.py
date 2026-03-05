"""Unit tests for the custom DataFrameModel."""

from datetime import date, datetime
from typing import Annotated

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.core.exceptions import SchemaValidationError
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel

# ---------------------------------------------------------------------------
# Helpers — define test schemas
# ---------------------------------------------------------------------------


class SimpleSchema(DataFrameModel):
    id: Annotated[str, Column(nullable=False, unique=True)]
    value: float
    flag: bool


class BoundsSchema(DataFrameModel):
    score: Annotated[int, Column(ge=0, le=100)]
    temp: Annotated[float, Column(gt=-273.15)]


class DateBoundsSchema(DataFrameModel):
    event_date: Annotated[date, Column(ge=date(2020, 1, 1), le=date(2030, 12, 31))]


class IsinSchema(DataFrameModel):
    category: Annotated[str, Column(isin=["A", "B", "C"])]


class DtypeOverrideSchema(DataFrameModel):
    small_int: Annotated[int, Column(dtype=pl.Int16())]
    tags: Annotated[list, Column(dtype=pl.List(pl.String))]
    data: Annotated[bytes, Column(dtype=pl.Binary())]
    ts: Annotated[datetime, Column(dtype=pl.Datetime("us", "UTC"))]
    count: Annotated[int, Column(dtype=pl.UInt32())]


class NameOverrideSchema(DataFrameModel):
    col_with_parens: Annotated[str, Column(name="col_(with_parens)")]
    normal: int


# ---------------------------------------------------------------------------
# polars_schema()
# ---------------------------------------------------------------------------


class TestPolarsSchema:
    def test_simple_schema(self) -> None:
        schema = SimpleSchema.polars_schema()
        assert schema == pl.Schema({"id": pl.String, "value": pl.Float64, "flag": pl.Boolean})

    def test_dtype_override(self) -> None:
        schema = DtypeOverrideSchema.polars_schema()
        assert schema["small_int"] == pl.Int16
        assert schema["tags"] == pl.List(pl.String)
        assert schema["data"] == pl.Binary
        assert schema["ts"] == pl.Datetime("us", "UTC")
        assert schema["count"] == pl.UInt32

    def test_name_override_uses_custom_name(self) -> None:
        schema = NameOverrideSchema.polars_schema()
        assert "col_(with_parens)" in schema
        assert "col_with_parens" not in schema
        assert "normal" in schema


# ---------------------------------------------------------------------------
# Schema checks — pass
# ---------------------------------------------------------------------------


class TestSchemaPass:
    def test_valid_dataframe(self) -> None:
        df = pl.DataFrame({"id": ["a", "b"], "value": [1.0, 2.0], "flag": [True, False]})
        result = SimpleSchema.validate(df)
        assert result.equals(df)

    def test_valid_lazyframe(self) -> None:
        lf = pl.DataFrame({"id": ["a"], "value": [1.0], "flag": [True]}).lazy()
        result = SimpleSchema.validate_lazy(lf)
        assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# Schema checks — fail
# ---------------------------------------------------------------------------


class TestSchemaFail:
    def test_missing_column(self) -> None:
        df = pl.DataFrame({"id": ["a"], "value": [1.0]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        assert any("Missing column: flag" in e for e in exc_info.value.errors)

    def test_extra_column(self) -> None:
        df = pl.DataFrame({"id": ["a"], "value": [1.0], "flag": [True], "extra": [1]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        assert any("Unexpected column: extra" in e for e in exc_info.value.errors)

    def test_type_mismatch(self) -> None:
        df = pl.DataFrame({"id": ["a"], "value": pl.Series([1], dtype=pl.Int64), "flag": [True]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        assert any("value:" in e and "Float64" in e for e in exc_info.value.errors)

    def test_lazyframe_schema_fail(self) -> None:
        lf = pl.DataFrame({"id": ["a"], "value": [1.0]}).lazy()
        with pytest.raises(SchemaValidationError):
            SimpleSchema.validate_lazy(lf)


# ---------------------------------------------------------------------------
# Value checks — nullable
# ---------------------------------------------------------------------------


class TestNullable:
    def test_non_nullable_with_nulls(self) -> None:
        df = pl.DataFrame({"id": [None, "b"], "value": [1.0, 2.0], "flag": [True, False]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        assert any("id:" in e and "null" in e for e in exc_info.value.errors)

    def test_nullable_column_accepts_nulls(self) -> None:
        df = pl.DataFrame({"id": ["a", "b"], "value": [1.0, None], "flag": [True, False]})
        SimpleSchema.validate(df)


# ---------------------------------------------------------------------------
# Value checks — unique
# ---------------------------------------------------------------------------


class TestUnique:
    def test_duplicate_values(self) -> None:
        df = pl.DataFrame({"id": ["a", "a"], "value": [1.0, 2.0], "flag": [True, False]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        assert any("id:" in e and "duplicate" in e for e in exc_info.value.errors)

    def test_unique_passes(self) -> None:
        df = pl.DataFrame({"id": ["a", "b"], "value": [1.0, 2.0], "flag": [True, False]})
        SimpleSchema.validate(df)


# ---------------------------------------------------------------------------
# Value checks — bounds (ge, le, gt, lt)
# ---------------------------------------------------------------------------


class TestBounds:
    def test_ge_violation(self) -> None:
        df = pl.DataFrame({"score": [-1], "temp": [0.0]})
        with pytest.raises(SchemaValidationError) as exc_info:
            BoundsSchema.validate(df)
        assert any("score:" in e and "< 0" in e for e in exc_info.value.errors)

    def test_le_violation(self) -> None:
        df = pl.DataFrame({"score": [101], "temp": [0.0]})
        with pytest.raises(SchemaValidationError) as exc_info:
            BoundsSchema.validate(df)
        assert any("score:" in e and "> 100" in e for e in exc_info.value.errors)

    def test_gt_violation(self) -> None:
        df = pl.DataFrame({"score": [50], "temp": [-273.15]})
        with pytest.raises(SchemaValidationError) as exc_info:
            BoundsSchema.validate(df)
        assert any("temp:" in e and "<=" in e for e in exc_info.value.errors)

    def test_bounds_pass(self) -> None:
        df = pl.DataFrame({"score": [0, 50, 100], "temp": [0.0, 100.0, -273.14]})
        BoundsSchema.validate(df)


# ---------------------------------------------------------------------------
# Value checks — date bounds
# ---------------------------------------------------------------------------


class TestDateBounds:
    def test_date_ge_violation(self) -> None:
        df = pl.DataFrame({"event_date": [date(2019, 12, 31)]})
        with pytest.raises(SchemaValidationError) as exc_info:
            DateBoundsSchema.validate(df)
        assert any("event_date:" in e for e in exc_info.value.errors)

    def test_date_le_violation(self) -> None:
        df = pl.DataFrame({"event_date": [date(2031, 1, 1)]})
        with pytest.raises(SchemaValidationError) as exc_info:
            DateBoundsSchema.validate(df)
        assert any("event_date:" in e for e in exc_info.value.errors)

    def test_date_bounds_pass(self) -> None:
        df = pl.DataFrame({"event_date": [date(2025, 6, 15)]})
        DateBoundsSchema.validate(df)


# ---------------------------------------------------------------------------
# Value checks — isin
# ---------------------------------------------------------------------------


class TestIsin:
    def test_isin_violation(self) -> None:
        df = pl.DataFrame({"category": ["A", "X"]})
        with pytest.raises(SchemaValidationError) as exc_info:
            IsinSchema.validate(df)
        assert any("category:" in e and "outside" in e for e in exc_info.value.errors)

    def test_isin_pass(self) -> None:
        df = pl.DataFrame({"category": ["A", "B", "C"]})
        IsinSchema.validate(df)

    def test_isin_null_ignored(self) -> None:
        df = pl.DataFrame({"category": ["A", None]})
        IsinSchema.validate(df)


# ---------------------------------------------------------------------------
# Complex dtype overrides
# ---------------------------------------------------------------------------


class TestDtypeOverride:
    def test_list_type(self) -> None:
        df = pl.DataFrame(
            {
                "small_int": pl.Series([1], dtype=pl.Int16),
                "tags": [["a", "b"]],
                "data": [b"\x00"],
                "ts": pl.Series([datetime(2025, 1, 1)], dtype=pl.Datetime("us", "UTC")),
                "count": pl.Series([42], dtype=pl.UInt32),
            }
        )
        DtypeOverrideSchema.validate(df)

    def test_dtype_mismatch_int16_vs_int64(self) -> None:
        df = pl.DataFrame(
            {
                "small_int": pl.Series([1], dtype=pl.Int64),
                "tags": [["a"]],
                "data": [b"\x00"],
                "ts": pl.Series([datetime(2025, 1, 1)], dtype=pl.Datetime("us", "UTC")),
                "count": pl.Series([1], dtype=pl.UInt32),
            }
        )
        with pytest.raises(SchemaValidationError) as exc_info:
            DtypeOverrideSchema.validate(df)
        assert any("small_int:" in e for e in exc_info.value.errors)


# ---------------------------------------------------------------------------
# Name override
# ---------------------------------------------------------------------------


class TestNameOverride:
    def test_validates_with_overridden_name(self) -> None:
        df = pl.DataFrame({"col_(with_parens)": ["x"], "normal": [1]})
        NameOverrideSchema.validate(df)

    def test_fails_with_python_name(self) -> None:
        df = pl.DataFrame({"col_with_parens": ["x"], "normal": [1]})
        with pytest.raises(SchemaValidationError):
            NameOverrideSchema.validate(df)


# ---------------------------------------------------------------------------
# Metaclass error — unknown type
# ---------------------------------------------------------------------------


class TestMetaclassError:
    def test_unknown_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="No Polars dtype mapping"):

            class BadSchema(DataFrameModel):
                col: dict


# ---------------------------------------------------------------------------
# Multiple errors collected
# ---------------------------------------------------------------------------


class TestMultipleErrors:
    def test_schema_and_value_errors_combined(self) -> None:
        df = pl.DataFrame({"id": [None, None], "value": [1.0, 2.0]})
        with pytest.raises(SchemaValidationError) as exc_info:
            SimpleSchema.validate(df)
        errors = exc_info.value.errors
        # Missing column (flag) + nullable violation (id) + uniqueness
        assert len(errors) >= 2
