INSERT INTO {schema}.{table} (
    code_insee_region, date_heure, libelle_region, nature, date, heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique,
    pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies,
    inserted_at
)
SELECT
    code_insee_region, date_heure, libelle_region, nature, date, heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique,
    pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies,
    NOW()
FROM {staging}
ON CONFLICT (code_insee_region, date_heure) DO UPDATE SET
    consommation = EXCLUDED.consommation,
    thermique = EXCLUDED.thermique,
    nucleaire = EXCLUDED.nucleaire,
    eolien = EXCLUDED.eolien,
    solaire = EXCLUDED.solaire,
    hydraulique = EXCLUDED.hydraulique,
    bioenergies = EXCLUDED.bioenergies,
    updated_at = NOW()
WHERE (
    {schema}.{table}.consommation,
    {schema}.{table}.solaire,
    {schema}.{table}.eolien
) IS DISTINCT FROM (
    EXCLUDED.consommation,
    EXCLUDED.solaire,
    EXCLUDED.eolien
);
