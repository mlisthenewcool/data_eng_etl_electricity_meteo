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
    name:
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
    df:
        DataFrame to validate.
    dataset_name:
        Used in the exception for diagnostics.

    Raises
    ------
    TransformValidationError
        If *df* is empty.
    """
    if df.is_empty():
        raise TransformValidationError(dataset_name, reason="DataFrame is empty after transform")


def prepare_silver(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Apply common silver pre-processing: snake_case rename + drop all-null columns.

    Called by the registry wrapper **before** the dataset-specific transform,
    so that all silver transforms receive clean, snake_case column names.

    Parameters
    ----------
    df:
        Raw DataFrame read from the bronze parquet.
    dataset_name:
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
