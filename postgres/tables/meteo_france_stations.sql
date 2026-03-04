-- params_solaires and params_eoliens are TEXT[] (arrays of parameter names).

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id                  TEXT PRIMARY KEY,
    nom                 TEXT,
    lieu_dit            TEXT,
    bassin              TEXT,
    date_debut          DATE,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    altitude            BIGINT,
    mesure_solaire      BOOLEAN,
    mesure_eolien       BOOLEAN,
    params_solaires     TEXT[],
    params_eoliens      TEXT[],
    nb_parametres       INTEGER,
    inserted_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_coords
    ON {schema}.{table}(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_solaire
    ON {schema}.{table}(mesure_solaire) WHERE mesure_solaire = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_eolien
    ON {schema}.{table}(mesure_eolien) WHERE mesure_eolien = TRUE;

-- GiST indexes for PostGIS KNN spatial queries (used by gold model
-- installations_renouvelables_avec_stations_meteo for nearest-station lookup).
CREATE INDEX IF NOT EXISTS idx_stations_geog_solaire
    ON {schema}.{table}
    USING GIST ((ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography))
    WHERE mesure_solaire = TRUE;

CREATE INDEX IF NOT EXISTS idx_stations_geog_eolien
    ON {schema}.{table}
    USING GIST ((ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography))
    WHERE mesure_eolien = TRUE;
