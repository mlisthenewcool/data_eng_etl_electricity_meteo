"""Transformations for ODRE eco2mix_cons_def dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.odre_eco2mix_cons_def")


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

    Parameters
    ----------
    landing_path:
        Path to the parquet file from landing layer

    Returns
    -------
    pl.DataFrame
        DataFrame ready for bronze layer
    """
    logger.debug("Apply bronze transformations", landing_path=landing_path)
    return pl.read_parquet(landing_path)


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE eco2mix_cons_def.

    Selects only the columns defined in the schema, dropping any spurious
    columns from the source (e.g., column_30 from the ODRE API).

    Args:
        latest_bronze_path: Path to the latest bronze parquet file

    Returns
    -------
        Silver layer DataFrame with exact schema
    """
    logger.debug("Apply silver transformations", latest_bronze_path=latest_bronze_path)
    df = pl.read_parquet(latest_bronze_path)

    # Select only expected columns in the correct order
    # This drops any extra columns (like column_30) from the source
    # TODO: df = df.select(list(SCHEMA_ODRE_ECO2MIX_CONS_DEF.names()))

    # Deduplicate on primary key (region + datetime)
    # Keep last occurrence (most recent data from source)
    # This handles DST transitions where ODRE API may return duplicate timestamps
    df = df.unique(subset=["code_insee_region", "date_heure"], keep="last")

    # TODO: Validate the output
    # validate_odre_eco2mix_cons_def(df)

    logger.debug("Silver transformation completed", row_count=len(df), columns=df.columns)
    return df
