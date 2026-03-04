{{
    config(
        materialized='view',
        schema='silver'
    )
}}

SELECT
    id AS station_id,
    nom AS station_nom,
    latitude AS station_lat,
    longitude AS station_lon,
    mesure_solaire,
    mesure_eolien,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography AS geog
FROM {{ source('silver', 'dim_stations_meteo') }}
