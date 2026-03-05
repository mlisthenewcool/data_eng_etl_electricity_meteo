"""Transformations for ODRE eco2mix_cons_def dataset."""

from datetime import datetime
from pathlib import Path
from typing import Annotated

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.shared import (
    deduplicate_on_composite_key,
    validate_source_columns,
)
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

logger = get_logger("transform.odre_eco2mix_cons_def")

# Column that contains non-numeric annotations in the consolidated source API.
# Cast to Int64 (BIGINT) in silver; non-castable values become null.
_NUMERIC_TEXT_COLUMNS = ["eolien"]


# ---------------------------------------------------------------------------
# Silver schema
# ---------------------------------------------------------------------------

ALL_SOURCE_COLUMNS: set[str] = {
    "bioenergies",
    "code_insee_region",
    "consommation",
    "date",
    "date_heure",
    "destockage_batterie",
    "ech_physiques",
    "eolien",
    "eolien_offshore",
    "eolien_terrestre",
    "heure",
    "hydraulique",
    "libelle_region",
    "nature",
    "nucleaire",
    "pompage",
    "solaire",
    "stockage_batterie",
    "tch_bioenergies",
    "tch_eolien",
    "tch_hydraulique",
    "tch_nucleaire",
    "tch_solaire",
    "tch_thermique",
    "tco_bioenergies",
    "tco_eolien",
    "tco_hydraulique",
    "tco_nucleaire",
    "tco_solaire",
    "tco_thermique",
    "thermique",
}

# All source columns are used in the silver output.
USED_SOURCE_COLUMNS: set[str] = ALL_SOURCE_COLUMNS


class SilverSchema(DataFrameModel):
    """Silver output contract for ODRE eco2mix consolidé définitif."""

    code_insee_region: Annotated[str, Column(nullable=False)]
    libelle_region: str
    nature: str
    date: str
    heure: str
    date_heure: Annotated[datetime, Column(dtype=pl.Datetime("us"), nullable=False)]
    consommation: int
    thermique: int
    nucleaire: int
    eolien: int
    solaire: int
    hydraulique: int
    pompage: int
    bioenergies: int
    ech_physiques: int
    stockage_batterie: int
    destockage_batterie: int
    eolien_terrestre: int
    eolien_offshore: int
    tco_thermique: float
    tch_thermique: float
    tco_nucleaire: float
    tch_nucleaire: float
    tco_eolien: float
    tch_eolien: float
    tco_solaire: float
    tch_solaire: float
    tco_hydraulique: float
    tch_hydraulique: float
    tco_bioenergies: float
    tch_bioenergies: float


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

    Simply scans parquet from landing as-is.

    Parameters
    ----------
    landing_path
        Path to the parquet file from landing layer.

    Returns
    -------
    pl.LazyFrame
        LazyFrame ready for bronze layer.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read failure (corrupt file, schema mismatch, etc.).
    OSError
        If *landing_path* does not exist or is not readable.
    """
    logger.debug("Apply bronze transformations", landing_path=landing_path)
    return pl.scan_parquet(landing_path)


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
    validate_source_columns(df, ALL_SOURCE_COLUMNS, "odre_eco2mix_cons_def")

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

    result = df.select(SilverSchema.polars_schema().names())
    logger.debug("Silver transformation completed", row_count=len(result), columns=result.columns)
    SilverSchema.validate(result)
    return result


# ---------------------------------------------------------------------------
# Transform spec (collected by registry)
# ---------------------------------------------------------------------------

SPEC = DatasetTransformSpec(
    name="odre_eco2mix_cons_def",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    all_source_columns=frozenset(ALL_SOURCE_COLUMNS),
    used_source_columns=frozenset(USED_SOURCE_COLUMNS),
    silver_schema=SilverSchema,
)
