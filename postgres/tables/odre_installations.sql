-- "date_mise_enservice_(format_date)" is a verbatim column name from the ODRE source API.
--
-- Previous schema used a 24-column subset: id_peps, nom_installation, code_iris,
-- code_insee (→ code_insee_commune), commune, code_departement, departement,
-- code_region, region, code_filiere, filiere, code_technologie, technologie,
-- puissance_max_installee (→ puis_max_installee), puissance_max_raccordement
-- (→ puis_max_rac), nb_groupes, date_mise_en_service, date_raccordement,
-- date_deraccordement, regime, gestionnaire, code_epci, epci, est_renouvelable,
-- type_energie, est_actif. Current schema keeps all source API columns.

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id_peps                                 TEXT PRIMARY KEY,
    nom_installation                        TEXT,
    code_eic_resource_object                TEXT,
    code_iris                               TEXT,
    code_insee_commune                      TEXT,
    commune                                 TEXT,
    code_epci                               TEXT,
    epci                                    TEXT,
    code_departement                        TEXT,
    departement                             TEXT,
    code_region                             TEXT,
    region                                  TEXT,
    code_iris_commune_implantation          TEXT,
    code_insee_commune_implantation         TEXT,
    code_s3_renr                            TEXT,
    date_raccordement                       DATE,
    date_deraccordement                     DATE,
    date_mise_en_service                    DATE,
    date_debut_version                      DATE,
    poste_source                            TEXT,
    tension_raccordement                    TEXT,
    mode_raccordement                       TEXT,
    code_filiere                            TEXT,
    filiere                                 TEXT,
    code_combustible                        TEXT,
    combustible                             TEXT,
    codes_combustibles_secondaires          TEXT,
    combustibles_secondaires                TEXT,
    code_technologie                        TEXT,
    technologie                             TEXT,
    type_stockage                           TEXT,
    puis_max_installee                      DOUBLE PRECISION,
    puis_max_rac_charge                     TEXT,
    puis_max_charge                         TEXT,
    puis_max_rac                            DOUBLE PRECISION,
    puis_max_installee_dis_charge           TEXT,
    nb_groupes                              BIGINT,
    nb_installations                        BIGINT,
    regime                                  TEXT,
    energie_stockable                       TEXT,
    capacite_reservoir                      TEXT,
    hauteur_chute                           TEXT,
    productible                             TEXT,
    debit_maximal                           TEXT,
    code_gestionnaire                       TEXT,
    gestionnaire                            TEXT,
    energie_annuelle_glissante_injectee     BIGINT,
    energie_annuelle_glissante_produite     BIGINT,
    energie_annuelle_glissante_soutiree     BIGINT,
    max_puis                                TEXT,
    "date_mise_enservice_(format_date)"     DATE,
    est_renouvelable                        BOOLEAN,
    type_energie                            TEXT,
    est_actif                               BOOLEAN,
    est_agregation                          BOOLEAN,
    inserted_at                             TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_installations_iris
    ON {schema}.{table}(code_iris);

CREATE INDEX IF NOT EXISTS idx_dim_installations_type_energie
    ON {schema}.{table}(type_energie);

CREATE INDEX IF NOT EXISTS idx_dim_installations_filiere
    ON {schema}.{table}(code_filiere);

CREATE INDEX IF NOT EXISTS idx_dim_installations_region
    ON {schema}.{table}(code_region);

CREATE INDEX IF NOT EXISTS idx_dim_installations_renouvelable
    ON {schema}.{table}(est_renouvelable) WHERE est_renouvelable = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_installations_agregation
    ON {schema}.{table}(est_agregation) WHERE est_agregation = TRUE;
