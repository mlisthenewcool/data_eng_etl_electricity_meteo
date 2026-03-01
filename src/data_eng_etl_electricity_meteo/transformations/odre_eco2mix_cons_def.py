"""Transformations for ODRE eco2mix_cons_def dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.shared import deduplicate_on_composite_key

logger = get_logger("transform.odre_eco2mix_cons_def")

# Column that contains non-numeric annotations in the consolidated source API.
# Cast to Int64 (BIGINT) in silver; non-castable values become null.
_NUMERIC_TEXT_COLUMNS = ["eolien"]


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

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
    """Silver transformation for ODRE eco2mix_cons_def.

    Deduplicates on the composite primary key ``(code_insee_region, date_heure)``,
    keeping the last occurrence. This handles DST transitions where the ODRE API may
    return duplicate timestamps with updated values.

    Note: extra all-null columns from the source (e.g. ``column_30``) are dropped by
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
    # Cast columns that contain non-numeric annotations to Int64
    for col in _NUMERIC_TEXT_COLUMNS:
        if col in df.columns:
            before_nulls = df[col].null_count()
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
            introduced = df[col].null_count() - before_nulls
            if introduced > 0:
                logger.warning("Cast introduced nulls", column=col, new_nulls=introduced)

    df = deduplicate_on_composite_key(
        df,
        key_columns=["code_insee_region", "date_heure"],
        dataset_name="odre_eco2mix_cons_def",
    )

    logger.debug("Silver transformation completed", row_count=len(df), columns=df.columns)
    return df
