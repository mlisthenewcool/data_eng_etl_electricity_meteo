"""Transformations for ODRE installations dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.odre_installations")


# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

# Renewable energy filieres
FILIERES_RENOUVELABLES = ["SOLAI", "EOLIE", "HYDLQ", "BIOEN", "MARIN", "GEOTH"]

# Mapping from codeFiliere to simplified type
TYPE_ENERGIE_MAPPING = {
    "SOLAI": "solaire",
    "EOLIE": "eolien",
    "HYDLQ": "hydraulique",
    "BIOEN": "bioenergie",
    "MARIN": "marin",
    "GEOTH": "geothermie",
    "THERM": "thermique",
    "NUCLE": "nucleaire",
    "STOCK": "stockage",
    "AUTRE": "autre",
}


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE installations.

    Simply reads parquet from landing.

    Parameters
    ----------
    landing_path:
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
    logger.debug("Reading parquet from landing", landing_path=landing_path)
    return pl.read_parquet(landing_path)


# ---------------------------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------------------------


def transform_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Generates synthetic primary keys for aggregated installations and adds
    business flags for energy type classification.

    Transformations applied:

    - Flag ``est_agregation`` (``True`` when original ``id_peps`` is null).
    - Synthetic ``id_peps`` for aggregated rows via geographic cascade
      (IRIS → COM → DEP → REG → FR) + filière + sequence number.
    - Add ``est_renouvelable`` flag based on ``code_filiere``.
    - Add ``type_energie`` simplified classification via ``TYPE_ENERGIE_MAPPING``.
    - Add ``est_actif`` flag (``True`` when ``date_deraccordement`` is null).

    Parameters
    ----------
    df:
        Pre-processed bronze DataFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.DataFrame
        Enriched DataFrame with energy type flags.
    """
    logger.debug("Applying transformations", n_rows=len(df), n_cols=len(df.columns))

    # --- Synthetic key for aggregated installations (id_peps is NULL) ----------
    _geo_key = pl.coalesce(
        pl.concat_str([pl.lit("IRIS"), pl.col("code_iris")], separator="_"),
        pl.concat_str([pl.lit("COM"), pl.col("code_insee_commune")], separator="_"),
        pl.concat_str([pl.lit("DEP"), pl.col("code_departement")], separator="_"),
        pl.concat_str([pl.lit("REG"), pl.col("code_region")], separator="_"),
        pl.lit("FR"),
    )
    _base_key = pl.concat_str([pl.lit("AGR"), _geo_key, pl.col("code_filiere")], separator="_")

    n_null_peps = df["id_peps"].null_count()
    df = df.with_columns(
        pl.col("id_peps").is_null().alias("est_agregation"),
        _base_key.alias("_base_key"),
    )

    # Add per-group sequence number so each synthetic key is unique
    df = df.with_columns(
        pl.when(pl.col("est_agregation"))
        .then(
            pl.concat_str(
                [
                    pl.col("_base_key"),
                    pl.col("est_agregation").cum_sum().over("_base_key").cast(pl.Utf8),
                ],
                separator="_",
            )
        )
        .otherwise(pl.col("id_peps"))
        .alias("id_peps")
    ).drop("_base_key")

    logger.info("Synthetic keys generated for aggregated installations", n_synthetic=n_null_peps)

    # --- Business flags -------------------------------------------------------
    df_with_flags = df.with_columns(
        pl.col("code_filiere").is_in(FILIERES_RENOUVELABLES).alias("est_renouvelable"),
        pl.col("code_filiere")
        .replace_strict(TYPE_ENERGIE_MAPPING, default="autre")
        .alias("type_energie"),
        pl.col("date_deraccordement").is_null().alias("est_actif"),
    )

    logger.debug(
        "Silver transformation completed",
        n_rows=len(df_with_flags),
        n_renouvelables=df_with_flags["est_renouvelable"].sum(),
        n_actifs=df_with_flags["est_actif"].sum(),
    )

    return df_with_flags
