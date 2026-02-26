-- Silver table: ODRE regional electricity production (consolidated & final).
-- Strategy: snapshot (TRUNCATE + COPY on each load).
-- Primary key: (code_insee_region, date_heure).
--
-- Note: the "eolien" column is TEXT in this dataset (vs BIGINT in eco2mix_tr),
-- a known inconsistency in the ODRE source API for consolidated data.

CREATE TABLE IF NOT EXISTS silver.odre_eco2mix_cons_def (
    code_insee_region       TEXT            NOT NULL,
    libelle_region          TEXT,
    nature                  TEXT,
    "date"                  TEXT,
    heure                   TEXT,
    date_heure              TIMESTAMPTZ     NOT NULL,
    consommation            BIGINT,
    thermique               BIGINT,
    nucleaire               BIGINT,
    eolien                  TEXT,
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
    PRIMARY KEY (code_insee_region, date_heure)
);
