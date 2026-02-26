-- Silver table: IGN IRIS geographic units (population census boundaries).
-- Strategy: snapshot (TRUNCATE + COPY on each load).
-- Primary key: code_iris (9-character IRIS identifier).
--
-- geom_wkb stores the polygon geometry as WKB bytes in Lambert 93 (EPSG:2154).
-- centroid_lat / centroid_lon are the polygon centroid in WGS84 (EPSG:4326).
--
-- Future: geom_wkb could be migrated to a PostGIS geometry column (geometry(Polygon, 2154))
-- once PostGIS is enabled in the project database.

CREATE TABLE IF NOT EXISTS silver.ign_contours_iris (
    code_iris       TEXT PRIMARY KEY,
    nom_iris        TEXT,
    code_insee      TEXT,
    nom_commune     TEXT,
    type_iris       TEXT,
    geom_wkb        BYTEA,
    centroid_lat    DOUBLE PRECISION,
    centroid_lon    DOUBLE PRECISION
);
