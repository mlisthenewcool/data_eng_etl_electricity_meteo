"""DuckDB SQL translation of the 6 silver transforms and the gold model (V2 prototype).

Validated on 2026-10-24: row-by-row identical to the Polars
``DatasetTransformSpec.run_silver`` outputs (6 datasets) and to the PostGIS/dbt gold.
Source for the future dbt models.

Conventions shared by every transform:

- input relation ``src``: bronze columns renamed to snake_case (``prepare_columns``)
  plus ``_rn`` = ``file_row_number`` of the bronze Parquet
  (source order, needed for the "keep last" dedup and the installations synthetic keys);
- DuckDB 2.0 lambda syntax (``lambda x: ...``; ``x -> ...`` is rejected);
- ``geometry_always_xy = true`` must be set for ``ST_Distance_Sphere`` (gold).
"""

import re

DATASETS = [
    "meteo_france_climatologie",
    "odre_eco2mix_cons_def",
    "odre_eco2mix_tr",
    "odre_installations",
    "meteo_france_stations",
    "ign_contours_iris",
]

PK = {
    "meteo_france_climatologie": ["id_station", "date_heure"],
    "odre_eco2mix_tr": ["code_insee_region", "date_heure"],
    "odre_eco2mix_cons_def": ["code_insee_region", "date_heure"],
    "odre_installations": ["code_eic_resource_object"],
    "meteo_france_stations": ["id"],
    "ign_contours_iris": ["code_iris"],
}

_ECO = {
    "bioenergies",
    "code_insee_region",
    "consommation",
    "date",
    "date_heure",
    "destockage_batterie",
    "ech_physiques",
    "eolien",
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

# Expected source columns after snake_case rename
# (mirror of each module's _ALL_SOURCE_COLUMNS).
EXPECTED = {
    "meteo_france_climatologie": {
        "num_poste",
        "aaaammjjhh",
        "glo",
        "ins",
        "n",
        "ff",
        "dd",
        "fxi",
        "t",
        "tx",
        "tn",
        "td",
        "u",
        "rr1",
        "pstat",
        "pmer",
    },
    "odre_eco2mix_tr": _ECO,
    "odre_eco2mix_cons_def": _ECO | {"eolien_offshore", "eolien_terrestre"},
    "odre_installations": {
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
    },
    "meteo_france_stations": {
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
    },
    "ign_contours_iris": {
        "cleabs",
        "code_insee",
        "code_iris",
        "geometrie",
        "iris",
        "nom_commune",
        "nom_iris",
        "type_iris",
    },
}

SOLAIRES = [
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
EOLIENS = [
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
FILIERES = "('SOLAI', 'EOLIE', 'HYDLQ', 'BIOEN', 'MARIN', 'GEOTH')"
TYPE_ENERGIE = """CASE code_filiere WHEN 'SOLAI' THEN 'solaire' WHEN 'EOLIE' THEN 'eolien'
    WHEN 'HYDLQ' THEN 'hydraulique' WHEN 'BIOEN' THEN 'bioenergie' WHEN 'MARIN' THEN 'marin'
    WHEN 'GEOTH' THEN 'geothermie' WHEN 'THERM' THEN 'thermique' WHEN 'NUCLE' THEN 'nucleaire'
    WHEN 'STOCK' THEN 'stockage' WHEN 'AUTRE' THEN 'autre' END"""


# --------------------------------------------------------------------------------------
# prepare_silver equivalent
# --------------------------------------------------------------------------------------


def to_snake_case(name: str) -> str:
    """Convert *name* to snake_case (same rule as ``transformations.shared``)."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", s)
    return s.lower().replace(" ", "_").replace("-", "_")


def prepare_columns(dataset: str, cols: list[str], non_null_counts: dict[str, int]) -> list[str]:
    """Return kept source columns, dropping spurious all-null ones; raise on drift."""
    renamed = {c: to_snake_case(c) for c in cols}
    kept = [c for c in cols if renamed[c] in EXPECTED[dataset] or non_null_counts.get(c, 1) > 0]
    actual = {renamed[c] for c in kept}
    if actual != EXPECTED[dataset]:
        added, removed = actual - EXPECTED[dataset], EXPECTED[dataset] - actual
        raise RuntimeError(f"{dataset} source drift: added={added} removed={removed}")
    return kept


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


# --------------------------------------------------------------------------------------
# Dataset transforms (SELECT over ``src``)
# --------------------------------------------------------------------------------------


def transform(dataset: str) -> str:
    """Return the silver SELECT for *dataset* (keeps ``_rn`` for the dedup step)."""
    if dataset == "meteo_france_climatologie":
        return """
        SELECT num_poste AS id_station,
               strptime(aaaammjjhh || ' +0000', '%Y%m%d%H %z') AS date_heure,
               glo AS rayonnement_global, ins AS duree_insolation, n::SMALLINT AS nebulosite,
               ff AS vitesse_vent, dd::SMALLINT AS direction_vent, fxi AS rafale_max,
               t AS temperature, tx AS temperature_max, tn AS temperature_min,
               td AS point_de_rosee, u::SMALLINT AS humidite, rr1 AS precipitations,
               pstat AS pression_station, pmer AS pression_mer, _rn
        FROM src"""
    if dataset.startswith("odre_eco2mix"):
        text_cols = (
            ["pompage", "stockage_batterie", "destockage_batterie"]
            if dataset == "odre_eco2mix_tr"
            else ["eolien"]
        )
        casts = ", ".join(f"TRY_CAST({c} AS BIGINT) AS {c}" for c in text_cols)
        return f"""
        SELECT * REPLACE ({casts}, timezone('UTC', date_heure)::TIMESTAMP AS date_heure)
        FROM src"""
    if dataset == "odre_installations":
        return f"""
        WITH k AS (
            SELECT *, code_eic_resource_object IS NULL AS est_agregation,
                   'AGR_' || coalesce('IRIS_' || code_iris, 'COM_' || code_insee_commune,
                                      'DEP_' || code_departement, 'REG_' || code_region, 'FR')
                          || '_' || code_filiere AS _base_key
            FROM src)
        SELECT * EXCLUDE (_base_key, code_eic_resource_object),
            CASE WHEN est_agregation
                 THEN _base_key || '_' || CAST(sum(est_agregation::INT) OVER w AS VARCHAR)
                 ELSE code_eic_resource_object END AS code_eic_resource_object,
            code_filiere IN {FILIERES} AS est_renouvelable,
            coalesce({TYPE_ENERGIE}, 'autre') AS type_energie,
            date_deraccordement IS NULL AS est_actif
        FROM k
        WINDOW w AS (PARTITION BY _base_key ORDER BY _rn ROWS UNBOUNDED PRECEDING)"""
    if dataset == "meteo_france_stations":
        active = "lambda x: x.dateFin IS NULL OR x.dateFin = ''"
        solaires, eoliens = _sql_list(SOLAIRES), _sql_list(EOLIENS)
        return f"""
        WITH s AS (
            SELECT *, list_filter(positions, {active}) AS pos,
                   list_transform(list_filter(parametres, {active}), lambda x: x.nom) AS noms
            FROM src WHERE date_fin IS NULL OR date_fin = '')
        SELECT * EXCLUDE (pos, noms, date_debut),
            pos[1].latitude AS latitude, pos[1].longitude AS longitude,
            pos[1].altitude AS altitude,
            len(list_filter(noms, lambda n: list_contains({solaires}, n))) > 0 AS mesure_solaire,
            len(list_filter(noms, lambda n: list_contains({eoliens}, n))) > 0 AS mesure_eolien,
            list_filter(noms, lambda n: list_contains({solaires}, n)) AS params_solaires,
            list_filter(noms, lambda n: list_contains({eoliens}, n)) AS params_eoliens,
            len(noms)::UINTEGER AS nb_parametres,
            strptime(substr(date_debut, 1, 10), '%Y-%m-%d')::DATE AS date_debut
        FROM s WHERE pos[1].latitude IS NOT NULL"""
    if dataset == "ign_contours_iris":
        return """
        SELECT * EXCLUDE (c), ST_Y(c) AS centroid_lat, ST_X(c) AS centroid_lon FROM (
            SELECT *, ST_Transform(ST_Centroid(ST_GeomFromWKB(geometrie)),
                                   'EPSG:2154', 'EPSG:4326') AS c
            FROM src)"""
    raise KeyError(dataset)


# SilverSchema constraints (nullability + bounds) as a violation count, per dataset.
CHECKS = {
    "meteo_france_climatologie": """count_if(id_station IS NULL OR date_heure IS NULL)
        + count_if(rayonnement_global < 0) + count_if(duree_insolation NOT BETWEEN 0 AND 60)
        + count_if(nebulosite NOT BETWEEN 0 AND 9) + count_if(vitesse_vent < 0)
        + count_if(direction_vent NOT BETWEEN 0 AND 360) + count_if(rafale_max < 0)
        + count_if(temperature NOT BETWEEN -50 AND 60) + count_if(humidite NOT BETWEEN 0 AND 100)
        + count_if(precipitations < 0) + count_if(pression_mer NOT BETWEEN 940 AND 1060)""",
    "odre_eco2mix_cons_def": "count_if(consommation < 0) + count_if(code_insee_region IS NULL)",
    "odre_eco2mix_tr": "count_if(consommation < 0) + count_if(code_insee_region IS NULL)",
    "odre_installations": """count_if(code_eic_resource_object IS NULL)
        + count_if(puis_max_installee < 0) + count_if(nb_groupes < 0)""",
    "meteo_france_stations": (
        "count_if(id IS NULL OR nom IS NULL OR bassin IS NULL OR altitude IS NULL)"
    ),
    "ign_contours_iris": """count_if(type_iris NOT IN ('H', 'A', 'D', 'Z'))
        + count_if(centroid_lat NOT BETWEEN 41 AND 52)
        + count_if(centroid_lon NOT BETWEEN -6 AND 10)""",
}

# Gold: nearest capable station per active solar/wind installation.
# Ranking on the sphere (= PostGIS ``<->`` on geography), distance on the spheroid
# (= PostGIS ``ST_Distance(geography)``): identical to the dbt-postgres gold.
GOLD = """
WITH ir AS (
    SELECT i.code_eic_resource_object, i.nom_installation, i.code_iris, i.code_departement,
           i.code_region, i.type_energie, i.puis_max_installee,
           r.centroid_lat AS installation_lat, r.centroid_lon AS installation_lon
    FROM {inst} i JOIN {iris} r ON i.code_iris = r.code_iris
    WHERE i.est_renouvelable AND i.est_actif AND i.type_energie IN ('solaire', 'eolien')),
best AS (
    SELECT ir.code_eic_resource_object,
           arg_min({{'id': s.id, 'nom': s.nom, 'lat': s.latitude, 'lon': s.longitude}},
                   (ST_Distance_Sphere(ST_Point(ir.installation_lon, ir.installation_lat),
                                       ST_Point(s.longitude, s.latitude)), s.id)) AS b
    FROM ir JOIN {st} s ON (ir.type_energie = 'solaire' AND s.mesure_solaire)
                        OR (ir.type_energie = 'eolien' AND s.mesure_eolien)
    GROUP BY ir.code_eic_resource_object)
SELECT ir.*, b.id AS station_id, b.nom AS station_nom, b.lat AS station_lat, b.lon AS station_lon,
    round(ST_Distance_Spheroid(ST_Point(installation_lon, installation_lat),
                               ST_Point(b.lon, b.lat))::DECIMAL(18, 6) / 1000, 2) AS distance_km
FROM ir JOIN best USING (code_eic_resource_object)"""
