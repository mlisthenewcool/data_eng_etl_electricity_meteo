CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id_station          TEXT NOT NULL,
    date_heure          TIMESTAMPTZ NOT NULL,
    rayonnement_global  DOUBLE PRECISION,
    duree_insolation    DOUBLE PRECISION,
    nebulosite          SMALLINT,
    vitesse_vent        DOUBLE PRECISION,
    direction_vent      SMALLINT,
    rafale_max          DOUBLE PRECISION,
    temperature         DOUBLE PRECISION,
    temperature_max     DOUBLE PRECISION,
    temperature_min     DOUBLE PRECISION,
    point_de_rosee      DOUBLE PRECISION,
    humidite            SMALLINT,
    precipitations      DOUBLE PRECISION,
    pression_station    DOUBLE PRECISION,
    pression_mer        DOUBLE PRECISION,
    inserted_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (id_station, date_heure)
);

CREATE INDEX IF NOT EXISTS idx_meteo_horaire_date
    ON {schema}.{table}(date_heure);

CREATE INDEX IF NOT EXISTS idx_meteo_horaire_station
    ON {schema}.{table}(id_station);
