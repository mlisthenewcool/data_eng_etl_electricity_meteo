-- geometrie: polygon geometry as WKB bytes in Lambert 93 (EPSG:2154).
-- Converted to PostGIS geometry in dbt staging model (stg_dim_contours_iris).
-- centroid_lat/centroid_lon: polygon centroid in WGS84 (EPSG:4326).

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    code_iris       TEXT PRIMARY KEY,
    nom_iris        TEXT,
    code_insee      TEXT,
    nom_commune     TEXT,
    type_iris       TEXT,
    geometrie       BYTEA,
    centroid_lat    DOUBLE PRECISION,
    centroid_lon    DOUBLE PRECISION,
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_contours_iris_commune
    ON {schema}.{table}(code_insee);

CREATE INDEX IF NOT EXISTS idx_dim_contours_iris_coords
    ON {schema}.{table}(centroid_lat, centroid_lon);
