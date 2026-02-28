-- pompage, stockage_batterie, destockage_batterie are TEXT because the real-time
-- source sometimes contains non-numeric annotations (vs BIGINT in consolidated).

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    code_insee_region       TEXT            NOT NULL,
    libelle_region          TEXT,
    nature                  TEXT,
    "date"                  TEXT,
    heure                   TEXT,
    date_heure              TIMESTAMPTZ     NOT NULL,
    consommation            BIGINT,
    thermique               BIGINT,
    nucleaire               BIGINT,
    eolien                  BIGINT,
    solaire                 BIGINT,
    hydraulique             BIGINT,
    pompage                 TEXT,
    bioenergies             BIGINT,
    ech_physiques           BIGINT,
    stockage_batterie       TEXT,
    destockage_batterie     TEXT,
    tco_thermique           DOUBLE PRECISION,
    tch_thermique           DOUBLE PRECISION,
    tco_nucleaire           DOUBLE PRECISION,
    tch_nucleaire           DOUBLE PRECISION,
    tco_eolien              DOUBLE PRECISION,
    tch_eolien              DOUBLE PRECISION,
    tco_solaire             DOUBLE PRECISION,
    tch_solaire             DOUBLE PRECISION,
    tco_hydraulique         DOUBLE PRECISION,
    tch_hydraulique         DOUBLE PRECISION,
    tco_bioenergies         DOUBLE PRECISION,
    tch_bioenergies         DOUBLE PRECISION,
    inserted_at             TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (code_insee_region, date_heure)
);

CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_tr_date
    ON {schema}.{table}(date_heure);
