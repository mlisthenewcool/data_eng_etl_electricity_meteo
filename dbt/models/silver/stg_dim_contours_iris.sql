{{
    config(
        materialized='view',
        schema='silver'
    )
}}

SELECT
    code_iris,
    nom_iris,
    code_insee,
    nom_commune,
    type_iris,
    centroid_lat,
    centroid_lon
FROM {{ source('silver', 'dim_contours_iris') }}
