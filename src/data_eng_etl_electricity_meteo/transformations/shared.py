"""Shared transformation utilities."""

import re

import polars as pl

from data_eng_etl_electricity_meteo.core.exceptions import TransformValidationError
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
    # Add underscore before uppercase letters preceded by lowercase/digit
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Handle acronyms (e.g. EPCICommune -> EPCI_Commune)
    s = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", s)
    # Lowercase and replace spaces/hyphens with underscores
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


def validate_no_nulls(df: pl.DataFrame, column: str | list[str], dataset_name: str) -> None:
    """Raise if *column* contains any null values.

    Parameters
    ----------
    df
        DataFrame to validate.
    column
        Column name (or list of column names) to check.
    dataset_name
        Used in the exception for diagnostics.

    Raises
    ------
    TransformValidationError
        If any checked column has at least one null.
    """
    columns = [column] if isinstance(column, str) else column
    for col in columns:
        null_count = df[col].null_count()
        if null_count > 0:
            raise TransformValidationError(
                dataset_name, reason=f"Column '{col}' has {null_count} null values"
            )


def validate_unique(df: pl.DataFrame, column: str | list[str], dataset_name: str) -> None:
    """Raise if *column* contains duplicate values.

    When *column* is a list, checks uniqueness of the composite key
    (i.e. the combination of all listed columns).

    Parameters
    ----------
    df
        DataFrame to validate.
    column
        Column name (or list of column names) to check for uniqueness.
    dataset_name
        Used in the exception for diagnostics.

    Raises
    ------
    TransformValidationError
        If the (composite) key has at least one duplicate.
    """
    if isinstance(column, str):
        n_dupes = len(df) - df[column].n_unique()
        label = f"Column '{column}'"
    else:
        n_dupes = df.select(column).is_duplicated().sum()
        label = f"Columns {column}"
    if n_dupes > 0:
        raise TransformValidationError(
            dataset_name, reason=f"{label} has {n_dupes} duplicate values"
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
            dataset_name=dataset_name,
            removed=removed,
            remaining=len(df),
            key_columns=key_columns,
        )
    return df


def prepare_silver(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Apply common silver pre-processing: snake_case rename + drop all-null columns.

    Called by the registry wrapper **before** the dataset-specific transform, so that
    all silver transforms receive clean, snake_case column names.

    Parameters
    ----------
    df
        Raw DataFrame read from the bronze parquet.
    dataset_name
        Dataset identifier (for logging).

    Returns
    -------
    pl.DataFrame
        DataFrame with snake_case columns and spurious all-null columns removed.
    """
    df = df.rename(to_snake_case)

    # Drop columns that are entirely null — these are spurious columns injected by
    # some source APIs (e.g. column_30 / column_68 from the ODRE eco2mix parquet).
    # Logged as a warning so operators are aware of structural drift in the source.
    null_cols = [col for col in df.columns if df[col].is_null().all()]
    if null_cols:
        logger.warning("Dropping all-null columns from source", dropped_columns=null_cols)
        df = df.drop(null_cols)

    logger.debug("Common silver pre-processing applied", dataset_name=dataset_name)
    return df
