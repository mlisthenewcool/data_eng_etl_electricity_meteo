"""Transformations for ODRE eco2mix_tr dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.odre_eco2mix_tr")


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_tr.

    Simply reads parquet from landing as-is.

    Parameters
    ----------
    landing_path
        Path to the parquet file from landing layer.

    Returns
    -------
    pl.DataFrame
        DataFrame ready for bronze layer.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read failure (corrupt file, schema mismatch, etc.).
    OSError
        If *landing_path* does not exist or is not readable.
    """
    logger.debug("Apply bronze transformations", landing_path=landing_path)
    return pl.read_parquet(landing_path)


# ---------------------------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------------------------


def transform_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Silver transformation for ODRE eco2mix_tr.

    Deduplicates on the composite primary key ``(code_insee_region, date_heure)``,
    keeping the last occurrence. This handles DST transitions where the ODRE API may
    return duplicate timestamps with updated values (e.g. 30 March 2025).

    Note: extra all-null columns from the source (e.g. ``column_68``) are dropped by
    ``prepare_silver`` before this function is called.

    Parameters
    ----------
    df
        Pre-processed bronze DataFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.DataFrame
        Deduplicated DataFrame ready for the silver layer.
    """
    # Select only expected columns in the correct order
    # This drops any extra columns (like column_68) from the source
    # TODO: df = df.select(list(SCHEMA_ODRE_ECO2MIX_TR.names()))

    # Deduplicate on primary key (region + datetime)
    # Keep last occurrence (most recent data from source)
    # This handles DST transitions where ODRE API may return duplicate timestamps
    # example : 30 March 2025
    df = df.unique(subset=["code_insee_region", "date_heure"], keep="last")

    # TODO: Validate the output
    # validate_odre_eco2mix_tr(df)

    logger.debug("Silver transformation completed", row_count=len(df), columns=df.columns)
    return df
