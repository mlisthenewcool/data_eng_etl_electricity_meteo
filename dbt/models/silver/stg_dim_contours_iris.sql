{{
    config(
        materialized='view',
        schema='silver'
    )
}}

select
    code_iris,
    nom_iris,
    code_insee,
    nom_commune,
    type_iris,
    centroid_lat,
    centroid_lon
from {{ source('silver', 'dim_contours_iris') }}
