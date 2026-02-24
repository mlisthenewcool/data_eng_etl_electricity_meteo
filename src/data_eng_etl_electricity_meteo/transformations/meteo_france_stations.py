"""Transformations for Météo France stations dataset."""

from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("transform.meteo_france_stations")

# Parameters relevant for solar energy production
# Based on analysis in notebooks/03_meteo_france_info_stations.py
PARAMS_SOLAIRES = [
    "RAYONNEMENT GLOBAL HORAIRE",
    "RAYONNEMENT GLOBAL HORAIRE EN TEMPS SOLAIRE VRAI",
    "RAYONNEMENT DIRECT HORAIRE",
    "RAYONNEMENT DIRECT HORAIRE EN TEMPS SOLAIRE VRAI",
    "DUREE D'INSOLATION HORAIRE",
    "DUREE D'INSOLATION HORAIRE EN TEMPS SOLAIRE VRAI",
    "NEBULOSITE TOTALE HORAIRE",
    "TEMPERATURE SOUS ABRI HORAIRE",
    "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE",
    "TEMPERATURE DU POINT DE ROSEE HORAIRE",
    "RAYONNEMENT GLOBAL QUOTIDIEN",
    "RAYONNEMENT DIRECT QUOTIDIEN",
]

# Parameters relevant for wind energy production
PARAMS_EOLIENS = [
    "VITESSE DU VENT HORAIRE",
    "DIRECTION DU VENT A 10 M HORAIRE",
    "MOYENNE DES VITESSES DU VENT A 10M",
    "VITESSE DU VENT MOYEN SUR 10 MN MAXI HORAIRE",
    "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES",
    "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES",
    "DIRECTION DU VENT MAXI INSTANTANE SUR 3 SECONDES",
    "VITESSE DU VENT A 2 METRES HORAIRE",
    "DIRECTION DU VENT A 2 METRES HORAIRE",
    "PRESSION STATION HORAIRE",
    "NOMBRE DE JOURS AVEC FXY>=8 M/S",
    "NOMBRE DE JOURS AVEC FXY>=10 M/S",
]


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for Meteo France stations.

    Simply reads JSON and converts to Parquet format.

    Parameters
    ----------
    landing_path:
        Path to the JSON file from landing layer.

    Returns
    -------
    pl.DataFrame
        DataFrame with raw station data.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read failure (malformed JSON, schema mismatch, etc.).
    OSError
        If *landing_path* does not exist or is not readable.
    """
    logger.debug("Reading JSON from landing", landing_path=landing_path)
    return pl.read_json(landing_path)


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for Meteo France stations.

    Flattens nested structures and enriches with renewable energy flags.

    Transformations applied:
    - Filter to active stations only (dateFin is empty)
    - Extract latest position (latitude, longitude, altitude)
    - Filter to active parameters only
    - Create boolean flags for solar/wind measurement capability
    - List available solar and wind parameters per station

    Parameters
    ----------
    latest_bronze_path:
        Path to the latest bronze parquet file.

    Returns
    -------
    pl.DataFrame
        Flattened DataFrame with measurement capability flags.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read or transformation failure.
    OSError
        If *latest_bronze_path* does not exist or is not readable.
    """
    logger.debug("Reading from bronze", latest_bronze_path=latest_bronze_path)
    df = pl.read_parquet(latest_bronze_path)

    logger.debug("Filtering active stations", total_stations=len(df))

    # Filter to active stations only
    df_active = df.filter((pl.col("dateFin").is_null()) | (pl.col("dateFin") == ""))

    logger.debug("Active stations filtered", active_stations=len(df_active))

    # Extract the latest position (the one with empty dateFin)
    # positions is a List of Structs: latitude, longitude, altitude, …
    df_with_position = df_active.with_columns(
        pl.col("positions")
        .list.eval(
            pl.element().filter(
                (pl.element().struct.field("dateFin").is_null())
                | (pl.element().struct.field("dateFin") == "")
            )
        )
        .list.first()
        .alias("current_position")
    ).with_columns(
        pl.col("current_position").struct.field("latitude").alias("latitude"),
        pl.col("current_position").struct.field("longitude").alias("longitude"),
        pl.col("current_position").struct.field("altitude").alias("altitude"),
    )

    # Filter parameters to active ones only and extract names
    df_with_params = df_with_position.with_columns(
        pl.col("parametres")
        .list.eval(
            pl.element().filter(
                (pl.element().struct.field("dateFin").is_null())
                | (pl.element().struct.field("dateFin") == "")
            )
        )
        .alias("parametres_actifs"),
    ).with_columns(
        pl.col("parametres_actifs")
        .list.eval(pl.element().struct.field("nom"))
        .alias("params_actifs_noms"),
    )

    # Create solar and wind capability flags
    df_with_flags = df_with_params.with_columns(
        pl.col("params_actifs_noms")
        .list.eval(pl.element().is_in(PARAMS_SOLAIRES))
        .list.any()
        .alias("mesure_solaire"),
        pl.col("params_actifs_noms")
        .list.eval(pl.element().is_in(PARAMS_EOLIENS))
        .list.any()
        .alias("mesure_eolien"),
        pl.col("params_actifs_noms")
        .list.eval(pl.element().filter(pl.element().is_in(PARAMS_SOLAIRES)))
        .alias("params_solaires"),
        pl.col("params_actifs_noms")
        .list.eval(pl.element().filter(pl.element().is_in(PARAMS_EOLIENS)))
        .alias("params_eoliens"),
        pl.col("params_actifs_noms").list.len().alias("nb_parametres"),
    )

    # Convert dateDebut to proper date
    df_final = df_with_flags.with_columns(
        pl.col("dateDebut").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("date_debut"),
    )

    # Select and rename final columns
    result = df_final.select(
        pl.col("id"),
        pl.col("nom"),
        pl.col("lieuDit").alias("lieu_dit"),
        pl.col("bassin"),
        pl.col("date_debut"),
        pl.col("latitude"),
        pl.col("longitude"),
        pl.col("altitude"),
        pl.col("mesure_solaire"),
        pl.col("mesure_eolien"),
        pl.col("params_solaires"),
        pl.col("params_eoliens"),
        pl.col("nb_parametres"),
    )

    logger.debug(
        "Silver transformation completed",
        n_stations=len(result),
        n_mesure_solaire=result["mesure_solaire"].sum(),
        n_mesure_eolien=result["mesure_eolien"].sum(),
    )

    return result
