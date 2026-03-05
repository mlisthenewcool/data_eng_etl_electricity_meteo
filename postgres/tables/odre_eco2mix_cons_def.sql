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
    pompage                 BIGINT,
    bioenergies             BIGINT,
    ech_physiques           BIGINT,
    stockage_batterie       BIGINT,
    destockage_batterie     BIGINT,
    eolien_terrestre        BIGINT,
    eolien_offshore         BIGINT,
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
    PRIMARY KEY (code_insee_region, date_heure)
);

CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_cons_def_date
    ON {schema}.{table}(date_heure);

-- No separate index on code_insee_region: B-tree composite indexes support
-- lookups on any left prefix, so the PK (code_insee_region, date_heure)
-- already serves region-only queries efficiently.
