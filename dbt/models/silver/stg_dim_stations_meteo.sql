{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['station_id'], 'unique': True},
            {'columns': ['geog'], 'type': 'gist'},
        ],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_stations_geog_solaire ON {{ this }} USING gist (geog) WHERE mesure_solaire",
            "CREATE INDEX IF NOT EXISTS idx_stations_geog_eolien ON {{ this }} USING gist (geog) WHERE mesure_eolien",
        ]
    )
}}

/*
    Materialized as table (not view) for the same reason as stg_dim_contours_iris:
    the geography column is built from raw lat/lon (Polars has no spatial type),
    and the gold KNN query (ORDER BY <->) requires a GiST index on geog.
*/

SELECT
    id AS station_id,
    nom AS station_nom,
    latitude AS station_lat,
    longitude AS station_lon,
    mesure_solaire,
    mesure_eolien,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography AS geog
FROM {{ source('silver', 'dim_stations_meteo') }}
