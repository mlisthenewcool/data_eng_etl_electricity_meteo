"""Transformations for Météo France stations dataset."""

from datetime import date
from pathlib import Path
from typing import Annotated

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

logger = get_logger("transform")


# --------------------------------------------------------------------------------------
# Domain constants (measurement parameters)
# --------------------------------------------------------------------------------------


# Parameters relevant for solar energy production
# Based on analysis in notebooks/03_meteo_france_info_stations.py
PARAMS_SOLAIRES: frozenset[str] = frozenset(
    {
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
    }
)

# Parameters relevant for wind energy production
PARAMS_EOLIENS: frozenset[str] = frozenset(
    {
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
    }
)


# --------------------------------------------------------------------------------------
# Geographic bounds (metropolitan France bounding box)
# --------------------------------------------------------------------------------------


_METRO_LAT_MIN = 41.0
_METRO_LAT_MAX = 52.0
_METRO_LON_MIN = -6.0
_METRO_LON_MAX = 10.0


# --------------------------------------------------------------------------------------
# Silver schema
# --------------------------------------------------------------------------------------


_ALL_SOURCE_COLUMNS: frozenset[str] = frozenset(
    {
        "bassin",
        "date_debut",
        "date_fin",
        "id",
        "lieu_dit",
        "nom",
        "parametres",
        "positions",
        "producteurs",
        "types_poste",
    }
)

# producteurs and types_poste are not used in the silver transform.
_USED_SOURCE_COLUMNS: frozenset[str] = _ALL_SOURCE_COLUMNS - {"producteurs", "types_poste"}


class SilverSchema(DataFrameModel):
    """Silver output contract for Météo France stations."""

    id: Annotated[str, Column(nullable=False, unique=True)]
    nom: str
    lieu_dit: str
    bassin: str
    date_debut: date
    # Includes overseas territories; geographic bounds removed.
    # Stations without position are filtered out upstream.
    latitude: Annotated[float, Column(nullable=False)]
    longitude: Annotated[float, Column(nullable=False)]
    altitude: int
    mesure_solaire: bool
    mesure_eolien: bool
    params_solaires: Annotated[list[str], Column(dtype=pl.List(pl.String))]
    params_eoliens: Annotated[list[str], Column(dtype=pl.List(pl.String))]
    nb_parametres: Annotated[int, Column(dtype=pl.UInt32())]


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
    """Bronze transformation for Météo France stations.

    Simply reads JSON and converts to Parquet format.

    Parameters
    ----------
    landing_path
        Path to the JSON file from landing layer.

    Returns
    -------
    pl.LazyFrame
        LazyFrame with raw station data.

    Raises
    ------
    polars.exceptions.PolarsError
        On any Polars read failure (malformed JSON, schema mismatch, etc.).
    OSError
        If *landing_path* does not exist or is not readable.
    """
    logger.debug("Reading JSON from landing")
    return pl.read_json(landing_path).lazy()


# --------------------------------------------------------------------------------------
# Silver transformation
# --------------------------------------------------------------------------------------


def transform_silver(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Silver transformation for Météo France stations.

    Flattens nested structures and enriches with renewable energy flags. Fully lazy —
    data quality checks are embedded as ``_warn_*`` / ``_diag_*`` diagnostic columns.

    Transformations applied:
    - Filter to active stations only (date_fin is empty)
    - Extract latest position (latitude, longitude, altitude)
    - Filter to active parameters only
    - Create boolean flags for solar/wind measurement capability
    - List available solar and wind parameters per station

    Parameters
    ----------
    lf
        Pre-processed LazyFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.LazyFrame
        Flattened LazyFrame with measurement capability flags.
    """
    # -- Filter active stations --------------------------------------------------------

    # date_fin is the snake_case version of the original dateFin column.
    # Nested struct fields (inside positions/parametres lists) keep their
    # original names — only top-level columns are renamed by prepare_silver.
    lf = lf.filter((pl.col("date_fin").is_null()) | (pl.col("date_fin") == ""))

    # -- Extract latest position -------------------------------------------------------

    # positions is a List of Structs: latitude, longitude, altitude, …
    # Nested struct field names are NOT renamed (still original casing).
    lf = lf.with_columns(
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

    # -- Data quality checks (embedded as diagnostic columns) --------------------------

    _overseas_filter = (
        (pl.col("latitude") < _METRO_LAT_MIN)
        | (pl.col("latitude") > _METRO_LAT_MAX)
        | (pl.col("longitude") < _METRO_LON_MIN)
        | (pl.col("longitude") > _METRO_LON_MAX)
    )
    lf = lf.with_columns(
        pl.col("latitude").is_null().sum().alias("_warn_dropped_no_position"),
        (pl.col("latitude").is_not_null() & _overseas_filter)
        .sum()
        .alias("_warn_overseas_stations"),
    )
    lf = lf.with_columns(
        (pl.len() - pl.col("_warn_dropped_no_position") - pl.col("_warn_overseas_stations")).alias(
            "_diag_metropolitan_stations"
        ),
    )

    # Drop stations without coordinates (unusable for spatial joins)
    lf = lf.filter(pl.col("latitude").is_not_null())

    # -- Extract active parameters and create flags ------------------------------------

    lf = lf.with_columns(
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

    lf = lf.with_columns(
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

    # -- Parse date --------------------------------------------------------------------

    lf = lf.with_columns(
        pl.col("date_debut").str.slice(0, 10).str.to_date("%Y-%m-%d"),
    )

    return lf


# --------------------------------------------------------------------------------------
# Transform spec (collected by registry)
# --------------------------------------------------------------------------------------


SPEC = DatasetTransformSpec(
    name="meteo_france_stations",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    primary_key=("id",),
    all_source_columns=_ALL_SOURCE_COLUMNS,
    used_source_columns=_USED_SOURCE_COLUMNS,
    silver_schema=SilverSchema,
)
