{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['code_iris'], 'unique': True},
            {'columns': ['code_insee']},
            {'columns': ['geom'], 'type': 'gist'},
        ]
    )
}}

/*
    Materialized as table (not view) for two reasons:

    1. Geometry conversion cost — ST_GeomFromWKB parses ~50k WKB polygons into
       PostGIS geometry objects. As a view this conversion runs on every SELECT;
       as a table it runs once per dbt run.

    2. GiST index — spatial queries (ST_Contains, ST_Intersects, KNN) require a
       GiST index on the geometry column, which is only possible on a table.

    Why geometrie arrives as BYTEA and not as geometry:
    The Python silver pipeline (Polars + psycopg COPY) writes geometrie as BYTEA
    because Polars has no geometry type. The conversion to PostGIS geometry is
    deferred to this dbt model, which is the first layer with PostGIS access.
    The source table (silver.dim_contours_iris) keeps raw WKB bytes; this model
    produces the exploitable geometry column.
*/

SELECT
    code_iris,
    nom_iris,
    code_insee,
    nom_commune,
    type_iris,
    centroid_lat,
    centroid_lon,
    ST_SetSRID(ST_GeomFromWKB(geometrie), 2154) AS geom
FROM {{ source('silver', 'dim_contours_iris') }}
