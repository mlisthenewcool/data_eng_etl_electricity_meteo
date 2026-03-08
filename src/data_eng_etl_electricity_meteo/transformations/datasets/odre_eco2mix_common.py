"""Shared silver transformation logic for ODRE eco2mix datasets.

Both eco2mix_tr (real-time) and eco2mix_cons_def (consolidated) share the same silver
pipeline: cast non-numeric text → deduplicate on ``(code_insee_region, date_heure)`` →
normalize ``date_heure`` to naive UTC µs → select and validate.
"""

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import DataFrameModel
from data_eng_etl_electricity_meteo.transformations.shared import (
    _collect,
    deduplicate_on_composite_key,
)

logger = get_logger("transform")

_ECO2MIX_KEY_COLUMNS: list[str] = ["code_insee_region", "date_heure"]


def transform_eco2mix_silver(
    lf: pl.LazyFrame,
    *,
    numeric_text_columns: frozenset[str],
    schema: type[DataFrameModel],
) -> pl.LazyFrame:
    """Apply the common eco2mix silver transformation pipeline.

    Fully lazy — dtype checks use ``collect_schema()``, cast-introduced null counts use
    cheap single-row aggregations, deduplication uses lazy ``unique()``.

    Parameters
    ----------
    lf
        Pre-processed LazyFrame (snake_case columns, all-null columns removed).
    numeric_text_columns
        Columns that may contain non-numeric text annotations to cast to Int64.
    schema
        DataFrameModel subclass defining the silver output contract.

    Returns
    -------
    pl.LazyFrame
        LazyFrame ready for the silver layer.
    """
    # -- Cast non-numeric text columns to Int64 ----------------------------------------

    schema_names = lf.collect_schema().names()
    cast_cols = [col for col in numeric_text_columns if col in schema_names]
    if cast_cols:
        # Detect cast-introduced nulls via cheap single-row aggregation:
        # count values that are non-null BEFORE cast but null AFTER.
        introduced_exprs = [
            (pl.col(c).is_not_null() & pl.col(c).cast(pl.Int64, strict=False).is_null())
            .sum()
            .alias(c)
            for c in cast_cols
        ]
        introduced_df = _collect(lf.select(introduced_exprs))
        for col in cast_cols:
            introduced: int = introduced_df[col].item()
            if introduced > 0:
                logger.warning("Cast introduced nulls", column=col, new_nulls=introduced)

        lf = lf.with_columns(pl.col(col).cast(pl.Int64, strict=False) for col in cast_cols)

    # -- Deduplicate on composite key --------------------------------------------------

    lf = deduplicate_on_composite_key(lf, key_columns=_ECO2MIX_KEY_COLUMNS)

    # -- Normalize date_heure to naive UTC microseconds --------------------------------
    # Source may send tz-aware (e.g. Europe/Berlin) or naive datetimes depending on the
    # export format. Normalize to naive UTC µs for a consistent silver schema.

    dh_dtype = lf.collect_schema()["date_heure"]
    if isinstance(dh_dtype, pl.Datetime) and dh_dtype.time_zone is not None:
        lf = lf.with_columns(
            pl.col("date_heure").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        )
    # Re-read dtype after potential timezone removal
    dh_dtype = lf.collect_schema()["date_heure"]
    if isinstance(dh_dtype, pl.Datetime) and dh_dtype.time_unit != "us":
        lf = lf.with_columns(pl.col("date_heure").cast(pl.Datetime("us")))

    # -- Select and return -------------------------------------------------------------

    return lf.select(schema.polars_schema().names())
