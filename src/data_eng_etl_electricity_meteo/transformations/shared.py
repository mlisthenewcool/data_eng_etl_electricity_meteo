"""Shared transformation utilities."""

import re

import polars as pl

from data_eng_etl_electricity_meteo.core.exceptions import (
    SourceSchemaDriftError,
    TransformValidationError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.shared")


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
    df: pl.DataFrame,
    expected_columns: set[str],
    dataset_name: str,
) -> None:
    """Raise if the source DataFrame columns differ from expectations.

    Called at the start of ``transform_silver()`` before any column selection, to detect
    upstream API schema drift early.

    Parameters
    ----------
    df
        Pre-processed bronze DataFrame (after ``prepare_silver``).
    expected_columns
        Set of column names expected in the source.
    dataset_name
        Dataset identifier (for the exception).

    Raises
    ------
    SourceSchemaDriftError
        If columns were added or removed compared to *expected_columns*.
    """
    actual = set(df.columns)
    added = sorted(actual - expected_columns)
    removed = sorted(expected_columns - actual)
    if added or removed:
        raise SourceSchemaDriftError(
            dataset_name=dataset_name,
            added=added,
            removed=removed,
        )


def deduplicate_on_composite_key(
    df: pl.DataFrame,
    key_columns: list[str],
    dataset_name: str,
) -> pl.DataFrame:
    """Deduplicate rows on a composite key, keeping the last occurrence.

    Handles DST (Daylight Saving Time) transitions where data sources may return
    duplicate timestamps: during the autumn clock change (last Sunday of October in
    France), the hour 2:00-3:00 occurs twice, producing duplicates on time-based keys.

    Parameters
    ----------
    df
        DataFrame to deduplicate.
    key_columns
        Column names forming the composite key.
    dataset_name
        Dataset identifier (for logging).

    Returns
    -------
    pl.DataFrame
        Deduplicated DataFrame.
    """
    before = len(df)
    df = df.unique(subset=key_columns, keep="last")
    removed = before - len(df)
    if removed > 0:
        logger.info(
            "Deduplicated rows",
            rows_removed=removed,
            rows_count=len(df),
            key_columns=key_columns,
        )
    return df


def prepare_silver(
    df: pl.DataFrame,
    dataset_name: str,
    expected_columns: frozenset[str] | None = None,
) -> pl.DataFrame:
    """Apply common silver pre-processing: snake_case rename + drop all-null columns.

    Called by ``DatasetTransformSpec.run_silver()`` before the dataset-specific
    transform, so that all silver transforms receive clean, snake_case column names.

    Parameters
    ----------
    df
        Raw DataFrame read from the bronze parquet.
    dataset_name
        Dataset identifier (for logging).
    expected_columns
        Columns that must be preserved even if entirely null.
        Spurious all-null columns (not in this set) are dropped.

    Returns
    -------
    pl.DataFrame
        DataFrame with snake_case columns and spurious all-null columns removed.
    """
    df = df.rename(to_snake_case)

    # Drop columns that are entirely null AND not expected by the transform.
    # Spurious all-null columns are injected by some source APIs
    # (e.g. column_30 / column_68 from the ODRE eco2mix parquet).
    # Expected columns are preserved even if all-null to avoid false
    # SourceSchemaDriftError in validate_source_columns.
    keep = expected_columns or set()
    null_cols = [col for col in df.columns if df[col].is_null().all() and col not in keep]
    if null_cols:
        logger.warning("Dropping all-null columns from source", dropped_columns=null_cols)
        df = df.drop(null_cols)

    logger.debug(
        "Silver pre-processing applied",
        rows_count=len(df),
        columns_count=len(df.columns),
    )
    return df
