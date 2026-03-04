{{
    config(
        materialized='view',
        schema='silver'
    )
}}

select
    id as station_id,
    nom as station_nom,
    latitude as station_lat,
    longitude as station_lon,
    mesure_solaire,
    mesure_eolien,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography as geog
from {{ source('silver', 'dim_stations_meteo') }}
