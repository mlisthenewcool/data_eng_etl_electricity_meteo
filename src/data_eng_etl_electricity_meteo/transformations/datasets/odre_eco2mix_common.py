"""Shared silver transformation logic for ODRE eco2mix datasets.

Both eco2mix_tr (real-time) and eco2mix_cons_def (consolidated) share the same silver
pipeline: cast non-numeric text → normalize ``date_heure`` to naive UTC µs.
Deduplication is handled centrally by ``DatasetTransformSpec.run_silver()``.
"""

import polars as pl


def transform_eco2mix_silver(
    lf: pl.LazyFrame,
    *,
    numeric_text_columns: frozenset[str],
) -> pl.LazyFrame:
    """Apply the common eco2mix silver transformation pipeline.

    Fully lazy — dtype checks use ``collect_schema()``, cast-introduced null counts are
    embedded as ``_warn_cast_nulls_*`` diagnostic columns.

    Parameters
    ----------
    lf
        Pre-processed LazyFrame (snake_case columns, all-null columns removed).
    numeric_text_columns
        Columns that may contain non-numeric text annotations to cast to Int64.

    Returns
    -------
    pl.LazyFrame
        LazyFrame ready for the silver layer.
    """
    # -- Cast non-numeric text columns to Int64 ----------------------------------------

    schema_names = lf.collect_schema().names()
    cast_cols = [col for col in numeric_text_columns if col in schema_names]
    if cast_cols:
        # Combine cast + introduced-null detection in a single with_columns.
        # Polars evaluates all expressions on the *pre-mutation* frame, so
        # the diagnostic reads the original (pre-cast) column value while the
        # cast expression replaces the column in the same pass.
        lf = lf.with_columns(
            *(pl.col(c).cast(pl.Int64, strict=False) for c in cast_cols),
            *(
                (pl.col(c).is_not_null() & pl.col(c).cast(pl.Int64, strict=False).is_null())
                .sum()
                .alias(f"_warn_cast_nulls_{c}")
                for c in cast_cols
            ),
        )

    # -- Normalize date_heure to naive UTC microseconds --------------------------------
    # Source may send tz-aware (e.g. Europe/Berlin) or naive datetime depending on the
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

    return lf
