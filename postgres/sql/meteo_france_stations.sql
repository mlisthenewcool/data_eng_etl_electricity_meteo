-- Silver table: Météo France weather stations metadata.
-- Strategy: snapshot (TRUNCATE + COPY on each load).
-- Primary key: id (station identifier).
--
-- params_solaires and params_eoliens are TEXT[] (arrays of parameter names).

CREATE TABLE IF NOT EXISTS silver.meteo_france_stations (
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
    nb_parametres       INTEGER
);
