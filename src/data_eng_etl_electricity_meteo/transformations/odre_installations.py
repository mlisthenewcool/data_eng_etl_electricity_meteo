"""Transformations for ODRE installations dataset."""

from datetime import date
from pathlib import Path
from typing import Annotated

import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.shared import validate_source_columns
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

logger = get_logger("transform.odre_installations")


# --------------------------------------------------------------------------------------
# Domain constants
# --------------------------------------------------------------------------------------


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


# --------------------------------------------------------------------------------------
# Silver schema
# --------------------------------------------------------------------------------------


# Source columns expected after prepare_silver (before computed columns are added).
ALL_SOURCE_COLUMNS: set[str] = {
    "capacite_reservoir",
    "code_combustible",
    "code_departement",
    "code_eic_resource_object",
    "code_epci",
    "code_filiere",
    "code_gestionnaire",
    "code_insee_commune",
    "code_insee_commune_implantation",
    "code_iris",
    "code_iris_commune_implantation",
    "code_region",
    "code_s3_renr",
    "code_technologie",
    "codes_combustibles_secondaires",
    "combustible",
    "combustibles_secondaires",
    "commune",
    "date_debut_version",
    "date_deraccordement",
    "date_mise_en_service",
    "date_mise_enservice_(format_date)",
    "date_raccordement",
    "debit_maximal",
    "departement",
    "energie_annuelle_glissante_injectee",
    "energie_annuelle_glissante_produite",
    "energie_annuelle_glissante_soutiree",
    "energie_stockable",
    "epci",
    "filiere",
    "gestionnaire",
    "hauteur_chute",
    "id_peps",
    "max_puis",
    "mode_raccordement",
    "nb_groupes",
    "nb_installations",
    "nom_installation",
    "poste_source",
    "productible",
    "puis_max_charge",
    "puis_max_installee",
    "puis_max_installee_dis_charge",
    "puis_max_rac",
    "puis_max_rac_charge",
    "regime",
    "region",
    "technologie",
    "tension_raccordement",
    "type_stockage",
}

# All source columns are used (directly or for computed flags).
USED_SOURCE_COLUMNS: set[str] = ALL_SOURCE_COLUMNS


class SilverSchema(DataFrameModel):
    """Silver output contract for ODRE installations."""

    id_peps: Annotated[str, Column(nullable=False, unique=True)]
    nom_installation: str
    code_eic_resource_object: str
    code_iris: str
    code_insee_commune: str
    commune: str
    code_epci: str
    epci: str
    code_departement: str
    departement: str
    code_region: str
    region: str
    code_iris_commune_implantation: str
    code_insee_commune_implantation: str
    code_s3_renr: str
    date_raccordement: date
    date_deraccordement: date
    date_mise_en_service: date
    date_debut_version: date
    poste_source: str
    tension_raccordement: str
    mode_raccordement: str
    code_filiere: str
    filiere: str
    code_combustible: str
    combustible: str
    codes_combustibles_secondaires: str
    combustibles_secondaires: str
    code_technologie: str
    technologie: str
    type_stockage: str
    puis_max_installee: float
    puis_max_rac_charge: str
    puis_max_charge: str
    puis_max_rac: float
    puis_max_installee_dis_charge: str
    nb_groupes: int
    nb_installations: int
    regime: str
    energie_stockable: str
    capacite_reservoir: str
    hauteur_chute: str
    productible: str
    debit_maximal: str
    code_gestionnaire: str
    gestionnaire: str
    energie_annuelle_glissante_injectee: int
    energie_annuelle_glissante_produite: int
    energie_annuelle_glissante_soutiree: int
    max_puis: str
    # Verbatim column name from the ODRE source API (with parentheses).
    date_mise_enservice_format_date: Annotated[
        date, Column(name="date_mise_enservice_(format_date)")
    ]
    est_renouvelable: bool
    type_energie: str
    est_actif: bool
    est_agregation: bool


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
    """Bronze transformation for ODRE installations.

    Simply scans parquet from landing.

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
    logger.debug("Reading parquet from landing")
    return pl.scan_parquet(landing_path)


# --------------------------------------------------------------------------------------
# Silver transformation
# --------------------------------------------------------------------------------------


def transform_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Generates synthetic primary keys for aggregated installations and adds business
    flags for energy type classification.

    Transformations applied:

    - Flag ``est_agregation`` (``True`` when original ``id_peps`` is null).
    - Synthetic ``id_peps`` for aggregated rows via geographic cascade (IRIS → COM → DEP
      → REG → FR) + filière + sequence number.
    - Add ``est_renouvelable`` flag based on ``code_filiere``.
    - Add ``type_energie`` simplified classification via ``TYPE_ENERGIE_MAPPING``.
    - Add ``est_actif`` flag (``True`` when ``date_deraccordement`` is null).

    Parameters
    ----------
    df
        Pre-processed bronze DataFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.DataFrame
        Enriched DataFrame with energy type flags.
    """
    validate_source_columns(df, ALL_SOURCE_COLUMNS, "odre_installations")

    logger.debug("Applying transformations", rows_count=len(df), columns_count=len(df.columns))

    # -- Synthetic key for aggregated installations (id_peps is NULL) ------------------

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

    logger.info(
        "Synthetic keys generated for aggregated installations",
        synthetic_count=n_null_peps,
    )

    # -- Business flags ----------------------------------------------------------------

    df_with_flags = df.with_columns(
        pl.col("code_filiere").is_in(FILIERES_RENOUVELABLES).alias("est_renouvelable"),
        pl.col("code_filiere")
        .replace_strict(TYPE_ENERGIE_MAPPING, default="autre")
        .alias("type_energie"),
        pl.col("date_deraccordement").is_null().alias("est_actif"),
    )

    result = df_with_flags.select(SilverSchema.polars_schema().names())

    logger.debug(
        "Silver transformation completed",
        rows_count=len(result),
        renouvelables_count=result["est_renouvelable"].sum(),
        actifs_count=result["est_actif"].sum(),
    )

    SilverSchema.validate(result)
    return result


# --------------------------------------------------------------------------------------
# Transform spec (collected by registry)
# --------------------------------------------------------------------------------------


SPEC = DatasetTransformSpec(
    name="odre_installations",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    all_source_columns=frozenset(ALL_SOURCE_COLUMNS),
    used_source_columns=frozenset(USED_SOURCE_COLUMNS),
    silver_schema=SilverSchema,
)
