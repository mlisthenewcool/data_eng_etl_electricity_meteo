"""Shared transformation utilities."""

import re

import polars as pl

from data_eng_etl_electricity_meteo.core.exceptions import (
    SourceSchemaDriftError,
    TransformValidationError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.shared")


def _collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame, asserting no GPU engine is active.

    Polars type stubs declare ``LazyFrame.collect()`` as returning ``InProcessQuery |
    DataFrame`` (GPU support). This wrapper narrows the type to ``DataFrame`` so callers
    avoid ``ty: ignore`` pragmas.

    Parameters
    ----------
    lf
        LazyFrame to collect.

    Returns
    -------
    pl.DataFrame
        Collected DataFrame.
    """
    df = lf.collect()
    assert isinstance(df, pl.DataFrame)
    return df


def to_snake_case(name: str) -> str:
    """Convert a CamelCase or mixed-case string to snake_case.

    Parameters
    ----------
    name
        Input string (CamelCase, mixed-case, space- or hyphen-separated).

    Returns
    -------
    str
        snake_case version of *name*.
    """
    # Separate camelCase boundaries (e.g. lastName → last_Name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Separate acronym boundaries (e.g. EPCICommune → EPCI_Commune)
    s = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", s)
    # Normalize separators and casing
    return s.lower().replace(" ", "_").replace("-", "_")


def validate_not_empty(df: pl.DataFrame, dataset_name: str) -> None:
    """Raise if the DataFrame has zero rows.

    Parameters
    ----------
    df
        DataFrame to validate.
    dataset_name
        Used in the exception for diagnostics.

    Raises
    ------
    TransformValidationError
        If *df* is empty.
    """
    if df.is_empty():
        raise TransformValidationError(dataset_name, reason="DataFrame is empty after transform")


def validate_source_columns(
    df_or_lf: pl.DataFrame | pl.LazyFrame,
    expected_columns: frozenset[str],
    dataset_name: str,
) -> None:
    """Raise if the source columns differ from expectations.

    Called at the start of ``transform_silver()`` before any column selection, to detect
    upstream API schema drift early.

    Parameters
    ----------
    df_or_lf
        Pre-processed DataFrame or LazyFrame (after ``prepare_silver``).
    expected_columns
        Set of column names expected in the source.
    dataset_name
        Dataset identifier (for the exception).

    Raises
    ------
    SourceSchemaDriftError
        If columns were added or removed compared to *expected_columns*.
    """
    if isinstance(df_or_lf, pl.LazyFrame):
        actual = set(df_or_lf.collect_schema().names())
    else:
        actual = set(df_or_lf.columns)
    added = sorted(actual - expected_columns)
    removed = sorted(expected_columns - actual)
    if added or removed:
        raise SourceSchemaDriftError(dataset_name, added=added, removed=removed)


def deduplicate_on_composite_key(
    lf: pl.LazyFrame,
    key_columns: list[str],
) -> pl.LazyFrame:
    """Deduplicate rows on a composite key, keeping the last occurrence.

    Handles DST (Daylight Saving Time) transitions where data sources may return
    duplicate timestamps: during the autumn clock change (last Sunday of October in
    France), the hour 2:00-3:00 occurs twice, producing duplicates on time-based keys.

    Row counts for the info log are obtained via cheap single-row aggregations
    (no full materialization).

    Parameters
    ----------
    lf
        LazyFrame to deduplicate.
    key_columns
        Column names forming the composite key.

    Returns
    -------
    pl.LazyFrame
        Deduplicated LazyFrame.
    """
    total: int = _collect(lf.select(pl.len())).item()
    lf = lf.unique(subset=key_columns, keep="last")
    deduped: int = _collect(lf.select(pl.len())).item()
    removed = total - deduped
    if removed > 0:
        logger.info(
            "Deduplicated rows",
            rows_removed=removed,
            rows_count=deduped,
            key_columns=key_columns,
        )
    return lf


def prepare_silver(
    lf: pl.LazyFrame,
    expected_columns: frozenset[str] | None = None,
) -> pl.LazyFrame:
    """Apply common silver pre-processing: snake_case rename + drop all-null columns.

    Called by ``DatasetTransformSpec.run_silver()`` before the dataset-specific
    transform, so that all silver transforms receive clean, snake_case column names.

    Operates lazily to avoid materializing the full dataset in memory. The all-null
    check collects a single-row aggregation (cheap) rather than the full data.

    Parameters
    ----------
    lf
        LazyFrame scanned from the bronze parquet.
    expected_columns
        Columns that must be preserved even if entirely null.
        Spurious all-null columns (not in this set) are dropped.

    Returns
    -------
    pl.LazyFrame
        LazyFrame with snake_case columns and spurious all-null columns removed.
    """
    lf = lf.rename(to_snake_case)

    # Drop columns that are entirely null AND not expected by the transform.
    # Spurious all-null columns are injected by some source APIs
    # (e.g. column_30 / column_68 from the ODRE eco2mix parquet).
    # Expected columns are preserved even if all-null to avoid false
    # SourceSchemaDriftError in validate_source_columns.
    keep = expected_columns or set()
    candidates = [c for c in lf.collect_schema().names() if c not in keep]

    if candidates:
        # Single-row aggregation: cheap even on large datasets.
        null_flags = _collect(lf.select(pl.col(c).is_null().all() for c in candidates))
        null_cols = [c for c in candidates if null_flags[c].item()]
        if null_cols:
            logger.warning(
                "Dropping all-null columns from source",
                dropped_columns=null_cols,
            )
            lf = lf.drop(null_cols)

    logger.debug(
        "Silver pre-processing applied",
        columns_count=len(lf.collect_schema().names()),
    )
    return lf
