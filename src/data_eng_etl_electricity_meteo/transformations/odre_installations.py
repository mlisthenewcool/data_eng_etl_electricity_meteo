"""Transformations for ODRE installations dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.shared import to_snake_case

logger = get_logger("transform.odre_installations")

# TODO: colonne id_peps nulle pour les installations aggrégées
# TODO: df.with_columns(pl.coalesce(["id_peps"], pl.concat_str([...], separator="_")))

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


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Adds business flags for energy type classification.

    Transformations applied:

    - Rename columns to snake_case (needed before business logic).
    - Add ``est_renouvelable`` flag based on ``code_filiere``.
    - Add ``type_energie`` simplified classification via ``TYPE_ENERGIE_MAPPING``.
    - Add ``est_actif`` flag (``True`` when ``date_deraccordement`` is null).

    Parameters
    ----------
    latest_bronze_path:
        Path to the latest bronze parquet file.

    Returns
    -------
    pl.DataFrame
        Normalized and enriched DataFrame.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read failure (corrupt file, schema mismatch, etc.).
    OSError
        If *latest_bronze_path* does not exist or is not readable.
    """
    logger.debug("Reading from bronze", latest_bronze_path=latest_bronze_path)
    df = pl.read_parquet(latest_bronze_path)

    logger.debug("Applying transformations", n_rows=len(df), n_cols=len(df.columns))

    # Rename to snake_case early: business logic below references snake_case names.
    # The registry wrapper will re-apply to_snake_case, which is a no-op.
    df_renamed = df.rename(to_snake_case)

    # Add business flags
    df_with_flags = df_renamed.with_columns(
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
