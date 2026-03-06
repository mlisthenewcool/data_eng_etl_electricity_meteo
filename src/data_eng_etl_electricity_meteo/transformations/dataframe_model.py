"""Declarative DataFrame validation, Pydantic-style.

Provides ``DataFrameModel`` — a base class whose type-annotated fields define the
expected Polars schema and per-column value constraints.
Subclasses declare columns with ``Annotated[T, Column(...)]`` and call ``validate()`` to
check a DataFrame against the contract.

See ``docs/dataframe_model_custom.md`` for design rationale.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Annotated, Any, ClassVar, get_args, get_origin, get_type_hints

import polars as pl

from data_eng_etl_electricity_meteo.core.exceptions import SchemaValidationError

# --------------------------------------------------------------------------------------
# Column descriptor
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Column:
    """Declarative constraints on a single Polars column.

    Equivalent of ``pydantic.Field()`` for DataFrame columns.

    Parameters
    ----------
    name
        Override the column name derived from the field name. Useful when the DataFrame
        column contains characters invalid in Python identifiers (e.g. parentheses).
    nullable
        Whether nulls are allowed (default ``True``).
    unique
        Whether all non-null values must be distinct.
    dtype
        Explicit Polars dtype override (e.g. ``pl.Int16``, ``pl.List(pl.String)``).
        When ``None``, the dtype is inferred from the Python type annotation.
    ge
        Inclusive lower bound.
    le
        Inclusive upper bound.
    gt
        Exclusive lower bound.
    lt
        Exclusive upper bound.
    isin
        Allowed values (non-null values outside this set are violations).
    """

    name: str | None = None
    nullable: bool = True
    unique: bool = False
    dtype: pl.DataType | None = None
    ge: int | float | date | datetime | None = None
    le: int | float | date | datetime | None = None
    gt: int | float | date | datetime | None = None
    lt: int | float | date | datetime | None = None
    isin: Sequence[Any] | None = None


# --------------------------------------------------------------------------------------
# Internal helpers
# --------------------------------------------------------------------------------------


_TYPE_MAP: dict[type, pl.DataType] = {
    int: pl.Int64(),
    float: pl.Float64(),
    str: pl.String(),
    bool: pl.Boolean(),
    bytes: pl.Binary(),
    datetime: pl.Datetime("us"),
    date: pl.Date(),
}


@dataclass(frozen=True, slots=True)
class _ResolvedColumn:
    """Column resolved at class-definition time: name + dtype + constraints."""

    name: str
    dtype: pl.DataType
    constraints: Column


def _parse_annotated(hint: Any) -> tuple[type, Column]:
    """Extract ``(python_type, Column)`` from a type hint.

    Supports plain types (``str``) and ``Annotated[str, Column(...)]``.
    """
    if get_origin(hint) is Annotated:
        args = get_args(hint)
        for arg in args[1:]:
            if isinstance(arg, Column):
                return args[0], arg
        return args[0], Column()
    return hint, Column()


# --------------------------------------------------------------------------------------
# Metaclass
# --------------------------------------------------------------------------------------


class DataFrameModelMeta(type):
    """Metaclass that extracts column definitions from type hints."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> type:
        """Build ``__columns__`` from type hints at class-definition time."""
        cls = super().__new__(mcs, name, bases, namespace)
        if not any(isinstance(b, DataFrameModelMeta) for b in bases):
            return cls

        hints = get_type_hints(cls, include_extras=True)
        columns: dict[str, _ResolvedColumn] = {}

        for field_name, hint in hints.items():
            if field_name.startswith("_"):
                continue
            python_type, col_meta = _parse_annotated(hint)
            dtype = col_meta.dtype or _TYPE_MAP.get(python_type)
            if dtype is None:
                msg = f"No Polars dtype mapping for {python_type!r} on '{field_name}'"
                raise TypeError(msg)
            col_name = col_meta.name or field_name
            columns[col_name] = _ResolvedColumn(col_name, dtype, col_meta)

        cls.__columns__ = columns  # type: ignore[attr-defined]
        return cls


# --------------------------------------------------------------------------------------
# Base model
# --------------------------------------------------------------------------------------


class DataFrameModel(metaclass=DataFrameModelMeta):
    """Declarative Polars DataFrame validation, Pydantic-style.

    Subclass and annotate fields to define the expected schema::

        class MySchema(DataFrameModel):
            id: Annotated[str, Column(nullable=False, unique=True)]
            value: Annotated[float, Column(ge=0)]

        MySchema.validate(df)
    """

    __columns__: ClassVar[dict[str, _ResolvedColumn]]

    @classmethod
    def polars_schema(cls) -> pl.Schema:
        """Return a ``pl.Schema`` derived from the column definitions."""
        return pl.Schema({c.name: c.dtype for c in cls.__columns__.values()})

    @classmethod
    def validate(cls, df: pl.DataFrame) -> pl.DataFrame:
        """Validate schema and values.

        Parameters
        ----------
        df
            DataFrame to validate.

        Returns
        -------
        pl.DataFrame
            The input DataFrame (unchanged) if validation passes.

        Raises
        ------
        SchemaValidationError
            If any schema or value check fails.
        """
        errors = cls._check_schema(df.schema) + cls._check_values(df)
        if errors:
            raise SchemaValidationError(errors)
        return df

    @classmethod
    def validate_lazy(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Validate schema only (no collect).

        Parameters
        ----------
        lf
            LazyFrame to validate.

        Returns
        -------
        pl.LazyFrame
            The input LazyFrame (unchanged) if schema checks pass.

        Raises
        ------
        SchemaValidationError
            If any schema check fails.
        """
        errors = cls._check_schema(lf.collect_schema())
        if errors:
            raise SchemaValidationError(errors)
        return lf

    # -- Pass 1: schema (works on LazyFrame) -------------------------------------------

    @classmethod
    def _check_schema(cls, schema: pl.Schema) -> list[str]:
        errors: list[str] = []
        expected = set(cls.__columns__)
        actual = set(schema.names())

        for name in sorted(expected - actual):
            errors.append(f"Missing column: {name}")
        for name in sorted(actual - expected):
            errors.append(f"Unexpected column: {name}")

        for name, col in cls.__columns__.items():
            if name in schema and schema[name] != col.dtype:
                errors.append(f"{name}: expected {col.dtype}, got {schema[name]}")
        return errors

    # -- Pass 2: values (DataFrame only) -----------------------------------------------

    @classmethod
    def _check_values(cls, df: pl.DataFrame) -> list[str]:
        errors: list[str] = []

        for name, col in cls.__columns__.items():
            if name not in df.columns:
                continue
            c = col.constraints
            series = df[name]

            if not c.nullable and series.null_count() > 0:
                errors.append(f"{name}: {series.null_count()} nulls")

            if c.unique:
                n_non_null = len(series) - series.null_count()
                n_unique_non_null = series.n_unique() - (1 if series.null_count() else 0)
                if n_unique_non_null != n_non_null:
                    errors.append(f"{name}: contains duplicates")

            errors.extend(_check_bounds(df, name, c))

            if c.isin is not None:
                n = df.filter(pl.col(name).is_not_null() & ~pl.col(name).is_in(c.isin)).height
                if n:
                    errors.append(f"{name}: {n} values outside allowed set")

        return errors


def _check_bounds(df: pl.DataFrame, name: str, c: Column) -> list[str]:
    """Check ge/le/gt/lt bounds on a single column."""
    errors: list[str] = []
    if c.ge is not None:
        n = df.filter(pl.col(name) < c.ge).height
        if n:
            errors.append(f"{name}: {n} values < {c.ge}")
    if c.le is not None:
        n = df.filter(pl.col(name) > c.le).height
        if n:
            errors.append(f"{name}: {n} values > {c.le}")
    if c.gt is not None:
        n = df.filter(pl.col(name) <= c.gt).height
        if n:
            errors.append(f"{name}: {n} values <= {c.gt}")
    if c.lt is not None:
        n = df.filter(pl.col(name) >= c.lt).height
        if n:
            errors.append(f"{name}: {n} values >= {c.lt}")
    return errors
