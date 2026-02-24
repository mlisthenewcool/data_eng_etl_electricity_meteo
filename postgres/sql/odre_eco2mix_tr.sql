-- Silver table: ODRE regional electricity production (real-time, hourly).
-- Strategy: incremental (upsert via INSERT ON CONFLICT).
-- Primary key: (code_insee_region, date_heure).
--
-- Each hourly load appends new measurements and corrects previously seen ones.
-- The upsert section below is executed by the loader after COPY to the staging
-- temp table "_staging_odre_eco2mix_tr" (created and dropped by the loader).
--
-- Note: pompage, stockage_batterie, destockage_batterie are TEXT in real-time
-- data (sometimes contain non-numeric annotations) vs BIGINT in consolidated.

CREATE TABLE IF NOT EXISTS silver.odre_eco2mix_tr (
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
    PRIMARY KEY (code_insee_region, date_heure)
);

-- BEGIN UPSERT
-- Merge staging table into final table. Executed after COPY to _staging_odre_eco2mix_tr.
INSERT INTO silver.odre_eco2mix_tr (
    code_insee_region, libelle_region, nature, "date", heure, date_heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique, pompage,
    bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies
)
SELECT
    code_insee_region, libelle_region, nature, "date", heure, date_heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique, pompage,
    bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies
FROM _staging_odre_eco2mix_tr
ON CONFLICT (code_insee_region, date_heure) DO UPDATE SET
    libelle_region      = EXCLUDED.libelle_region,
    nature              = EXCLUDED.nature,
    "date"              = EXCLUDED."date",
    heure               = EXCLUDED.heure,
    consommation        = EXCLUDED.consommation,
    thermique           = EXCLUDED.thermique,
    nucleaire           = EXCLUDED.nucleaire,
    eolien              = EXCLUDED.eolien,
    solaire             = EXCLUDED.solaire,
    hydraulique         = EXCLUDED.hydraulique,
    pompage             = EXCLUDED.pompage,
    bioenergies         = EXCLUDED.bioenergies,
    ech_physiques       = EXCLUDED.ech_physiques,
    stockage_batterie   = EXCLUDED.stockage_batterie,
    destockage_batterie = EXCLUDED.destockage_batterie,
    tco_thermique       = EXCLUDED.tco_thermique,
    tch_thermique       = EXCLUDED.tch_thermique,
    tco_nucleaire       = EXCLUDED.tco_nucleaire,
    tch_nucleaire       = EXCLUDED.tch_nucleaire,
    tco_eolien          = EXCLUDED.tco_eolien,
    tch_eolien          = EXCLUDED.tch_eolien,
    tco_solaire         = EXCLUDED.tco_solaire,
    tch_solaire         = EXCLUDED.tch_solaire,
    tco_hydraulique     = EXCLUDED.tco_hydraulique,
    tch_hydraulique     = EXCLUDED.tch_hydraulique,
    tco_bioenergies     = EXCLUDED.tco_bioenergies,
    tch_bioenergies     = EXCLUDED.tch_bioenergies;
