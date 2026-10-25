SELECT
    code_iris, nom_iris, type_iris,
    ST_Y(c) AS centroid_lat,
    ST_X(c) AS centroid_lon
FROM (
    SELECT *, ST_Transform(ST_Centroid(ST_GeomFromWKB(geometrie)), 'EPSG:2154', 'EPSG:4326', always_xy := true) AS c
    FROM {{ ref('bronze_ign_contours_iris') }}
)
