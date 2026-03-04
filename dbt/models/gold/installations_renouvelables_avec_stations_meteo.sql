{{
    config(
        materialized='table',
        schema='gold',
        indexes=[
            {'columns': ['id_peps'], 'unique': True},
            {'columns': ['type_energie']},
            {'columns': ['code_region']},
            {'columns': ['code_departement']},
            {'columns': ['station_id']},
        ]
    )
}}

/*
    Gold model: each active renewable installation (solar/wind) matched to its
    nearest weather station capable of measuring the relevant parameters.

    Uses PostGIS KNN operator (<->) with GiST indexes for fast nearest-neighbor
    lookup, then ST_Distance for the exact WGS84 distance in metres.
*/

-- @formatter:off
WITH installations_renouvelables AS (
    SELECT
        inst.id_peps,
        inst.nom_installation,
        inst.code_iris,
        inst.code_departement,
        inst.code_region,
        inst.type_energie,
        inst.puis_max_installee,
        iris.centroid_lat AS installation_lat,
        iris.centroid_lon AS installation_lon,
        ST_SetSRID(
            ST_MakePoint(iris.centroid_lon, iris.centroid_lat), 4326
        )::geography AS geog
    FROM {{ ref('stg_dim_installations') }} AS inst
    INNER JOIN {{ ref('stg_dim_contours_iris') }} AS iris
        ON inst.code_iris = iris.code_iris
    WHERE
        inst.est_renouvelable
        AND inst.est_actif
        AND inst.type_energie IN ('solaire', 'eolien')
)

SELECT
    ir.id_peps,
    ir.nom_installation,
    ir.code_iris,
    ir.code_departement,
    ir.code_region,
    ir.type_energie,
    ir.puis_max_installee,
    ir.installation_lat,
    ir.installation_lon,
    nearest.station_id,
    nearest.station_nom,
    nearest.station_lat,
    nearest.station_lon,
    ROUND(
        ST_Distance(ir.geog, nearest.geog)::numeric / 1000, 2
    ) AS distance_km
FROM installations_renouvelables AS ir
CROSS JOIN LATERAL (
    SELECT
        st.station_id,
        st.station_nom,
        st.station_lat,
        st.station_lon,
        st.geog
    FROM {{ ref('stg_dim_stations_meteo') }} AS st
    WHERE
        CASE ir.type_energie
            WHEN 'solaire' THEN st.mesure_solaire
            WHEN 'eolien'  THEN st.mesure_eolien
        END
    ORDER BY ir.geog <-> st.geog
    LIMIT 1
) AS nearest
-- @formatter:on