"""Transformations for Météo France climatologie dataset.

Bronze reads the merged parquet produced by ``utils.meteo_download`` (already pruned to
16 columns at download time) and casts all columns to the target types (Utf8 / Float64).

Silver renames columns, parses dates, and applies narrowing casts.

Source data from data.gouv.fr is already in final units (°C, m/s, hPa, mm).
No unit conversion is applied.
"""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.meteo_france_climatologie")


# ---------------------------------------------------------------------------
# Bronze: column selection + typing
# ---------------------------------------------------------------------------

BRONZE_COLUMNS: dict[str, pl.DataType | type[pl.DataType]] = {
    "NUM_POSTE": pl.Utf8,
    "AAAAMMJJHH": pl.Utf8,
    "GLO": pl.Float64,
    "INS": pl.Float64,
    "N": pl.Float64,
    "FF": pl.Float64,
    "DD": pl.Float64,
    "FXI": pl.Float64,
    "T": pl.Float64,
    "TX": pl.Float64,
    "TN": pl.Float64,
    "TD": pl.Float64,
    "U": pl.Float64,
    "RR1": pl.Float64,
    "PSTAT": pl.Float64,
    "PMER": pl.Float64,
}

# ---------------------------------------------------------------------------
# Silver: column mapping (snake_case after prepare_silver) → target
# ---------------------------------------------------------------------------

COLUMNS_MAPPING: dict[str, str] = {
    "num_poste": "id_station",
    "aaaammjjhh": "date_heure",
    "glo": "rayonnement_global",
    "ins": "duree_insolation",
    "n": "nebulosite",
    "ff": "vitesse_vent",
    "dd": "direction_vent",
    "fxi": "rafale_max",
    "t": "temperature",
    "tx": "temperature_max",
    "tn": "temperature_min",
    "td": "point_de_rosee",
    "u": "humidite",
    "rr1": "precipitations",
    "pstat": "pression_station",
    "pmer": "pression_mer",
}


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for Météo France climatologie.

    Reads the 16 columns from the merged parquet and casts numeric columns to Float64.
    ``strict=False`` converts sentinel strings (``""``, ``"mq"``) to null.

    Parameters
    ----------
    landing_path
        Path to the merged parquet file in the landing layer.

    Returns
    -------
    pl.DataFrame
        DataFrame with 16 typed columns ready for the bronze layer.
    """
    columns = list(BRONZE_COLUMNS.keys())
    logger.debug(
        "Reading merged parquet from landing",
        landing_path=landing_path,
        columns_selected=len(columns),
    )

    df = pl.read_parquet(landing_path, columns=columns)

    return df.with_columns(
        *(pl.col(c).cast(t, strict=False).alias(c) for c, t in BRONZE_COLUMNS.items())
    )


# ---------------------------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------------------------


def transform_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Silver transformation for Météo France climatologie.

    Applies column selection, renaming, date parsing, and narrowing casts.

    Source data is already in final units (°C, m/s, hPa, mm) — no unit conversion is
    applied.

    Transformations applied:

    - Select 16 meteorological columns from bronze
    - Rename to descriptive French names
    - Parse ``aaaammjjhh`` (``"2026022815"``) to UTC datetime
    - Narrowing casts: nébulosité, direction vent, humidité → Int16

    Parameters
    ----------
    df
        Pre-processed bronze DataFrame
        (snake_case columns, all-null columns removed by ``prepare_silver``).

    Returns
    -------
    pl.DataFrame
        Transformed DataFrame with 16 columns ready for the silver layer.
    """
    logger.debug("Starting silver transform", input_rows=len(df), input_columns=len(df.columns))

    # Select and rename columns
    source_cols = list(COLUMNS_MAPPING.keys())
    target_cols = list(COLUMNS_MAPPING.values())

    df = df.select(source_cols).rename(COLUMNS_MAPPING)

    # Parse date: "2026022815" → datetime(2026, 2, 28, 15, 0, 0, tzinfo=UTC)
    # Polars requires both %H and %M, so we append "00" for minutes
    df = df.with_columns(
        (pl.col("date_heure") + "00")
        .str.strptime(pl.Datetime("us", "UTC"), "%Y%m%d%H%M")
        .alias("date_heure"),
    )

    # Narrowing casts for integer-valued columns
    df = df.with_columns(
        pl.col("nebulosite").cast(pl.Int16, strict=False),
        pl.col("direction_vent").cast(pl.Int16, strict=False),
        pl.col("humidite").cast(pl.Int16, strict=False),
    )

    # Reorder columns to match the Postgres table schema
    df = df.select(target_cols)

    logger.debug(
        "Silver transformation completed",
        output_rows=len(df),
        output_columns=df.columns,
    )

    return df
