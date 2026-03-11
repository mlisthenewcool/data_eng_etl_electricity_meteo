"""Transformations for Météo France climatologie dataset.

Bronze reads the merged parquet produced by ``custom_downloads.meteo_climatologie``
(already pruned to 16 columns at download time) and casts all columns to the target
types (Utf8 / Float64).

Silver renames columns, parses dates, and applies narrowing casts.

Source data from data.gouv.fr is already in final units (°C, m/s, hPa, mm).
No unit conversion is applied.
"""

from datetime import datetime
from pathlib import Path
from typing import Annotated

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

logger = get_logger("transform")


# --------------------------------------------------------------------------------------
# Bronze: column selection + typing
# --------------------------------------------------------------------------------------


_BRONZE_COLUMNS: dict[str, pl.DataType | type[pl.DataType]] = {
    "NUM_POSTE": pl.String,
    "AAAAMMJJHH": pl.String,
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


# --------------------------------------------------------------------------------------
# Silver: column mapping (snake_case after prepare_silver) → target
# --------------------------------------------------------------------------------------


_COLUMNS_MAPPING: dict[str, str] = {
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


# --------------------------------------------------------------------------------------
# Silver schema
# --------------------------------------------------------------------------------------


_ALL_SOURCE_COLUMNS: frozenset[str] = frozenset(_COLUMNS_MAPPING.keys())

# All source columns are used (1:1 mapping to silver output).
_USED_SOURCE_COLUMNS: frozenset[str] = _ALL_SOURCE_COLUMNS


class SilverSchema(DataFrameModel):
    """Silver output contract for Météo France climatologie."""

    id_station: Annotated[str, Column(nullable=False)]
    date_heure: Annotated[
        datetime,
        Column(dtype=pl.Datetime("us", "UTC"), nullable=False),
    ]
    rayonnement_global: float
    duree_insolation: float
    nebulosite: Annotated[int, Column(dtype=pl.Int16(), ge=0, le=9)]
    vitesse_vent: Annotated[float, Column(ge=0)]
    direction_vent: Annotated[int, Column(dtype=pl.Int16(), ge=0, le=360)]
    rafale_max: float
    temperature: float
    temperature_max: float
    temperature_min: float
    point_de_rosee: float
    humidite: Annotated[int, Column(dtype=pl.Int16(), ge=0, le=100)]
    precipitations: Annotated[float, Column(ge=0)]
    pression_station: float
    pression_mer: float


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
    """Bronze transformation for Météo France climatologie.

    Returns a **LazyFrame** to avoid loading the full merged Parquet (~760 MB) into
    memory at once. The caller (``to_bronze``) uses ``sink_parquet`` so the Polars
    streaming engine processes data in chunks.

    Reads the 16 columns from the merged Parquet and casts numeric columns to Float64.
    Known sentinel strings (``""``, ``"mq"``) are replaced with null before casting.
    Uses ``strict=True`` so that any unexpected non-numeric value raises an error.

    Parameters
    ----------
    landing_path
        Path to the merged Parquet file in the landing layer.

    Returns
    -------
    pl.LazyFrame
        LazyFrame with 16 typed columns ready for the bronze layer.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read or cast failure (corrupt file, unexpected values).
    OSError
        If *landing_path* does not exist or is not readable.
    """
    columns = list(_BRONZE_COLUMNS.keys())
    logger.debug(
        "Reading merged parquet from landing (lazy)",
        columns_count=len(columns),
    )

    # Read schema eagerly (no data loaded) to detect which columns are still strings
    file_schema = pl.read_parquet_schema(landing_path)
    sentinel_values = ["", "mq"]
    string_numeric_cols = [
        c for c, t in _BRONZE_COLUMNS.items() if t != pl.String and file_schema.get(c) == pl.String
    ]

    lf = pl.scan_parquet(landing_path).select(columns)

    if string_numeric_cols:
        lf = lf.with_columns(
            pl.when(pl.col(c).is_in(sentinel_values)).then(None).otherwise(pl.col(c)).alias(c)
            for c in string_numeric_cols
        )

    return lf.with_columns(
        *(pl.col(c).cast(t, strict=True).alias(c) for c, t in _BRONZE_COLUMNS.items())
    )


# --------------------------------------------------------------------------------------
# Silver transformation
# --------------------------------------------------------------------------------------


def transform_silver(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Silver transformation for Météo France climatologie.

    Applies column selection, renaming, date parsing, and narrowing casts.
    All operations are lazy-compatible — Polars collapses the full query plan into a
    single optimized pass.

    Source data is already in final units (°C, m/s, hPa, mm) — no unit conversion is
    applied.

    Transformations applied:

    - Select 16 meteorological columns from bronze
    - Rename to descriptive French names
    - Parse ``aaaammjjhh`` (``"2026022815"``) to UTC datetime
    - Narrowing casts: nébulosité, direction vent, humidité → Int16

    Parameters
    ----------
    lf
        Pre-processed LazyFrame
        (snake_case columns, all-null columns removed by ``prepare_silver``).

    Returns
    -------
    pl.LazyFrame
        Transformed LazyFrame with 16 columns ready for the silver layer.
    """
    # -- Select and rename columns -----------------------------------------------------

    source_cols = list(_COLUMNS_MAPPING.keys())
    lf = lf.select(source_cols).rename(_COLUMNS_MAPPING)

    # -- Parse date --------------------------------------------------------------------

    # "2026022815" → datetime(2026, 2, 28, 15, 0, 0, tzinfo=UTC)
    # Polars requires both %H and %M, so we append "00" for minutes
    lf = lf.with_columns(
        (pl.col("date_heure") + "00")
        .str.strptime(pl.Datetime("us", "UTC"), "%Y%m%d%H%M")
        .alias("date_heure"),
    )

    # -- Narrowing casts to Int16 ------------------------------------------------------

    lf = lf.with_columns(
        pl.col("nebulosite").cast(pl.Int16, strict=True),
        pl.col("direction_vent").cast(pl.Int16, strict=True),
        pl.col("humidite").cast(pl.Int16, strict=True),
    )

    return lf


# --------------------------------------------------------------------------------------
# Transform spec (collected by registry)
# --------------------------------------------------------------------------------------


SPEC = DatasetTransformSpec(
    name="meteo_france_climatologie",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    primary_key=("id_station", "date_heure"),
    all_source_columns=_ALL_SOURCE_COLUMNS,
    used_source_columns=_USED_SOURCE_COLUMNS,
    silver_schema=SilverSchema,
)
