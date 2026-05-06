# Qualité de données : philosophie et décisions

Ce document trace la **philosophie de validation** appliquée à la couche silver
et les **décisions notables** issues de la confrontation des contraintes
théoriques aux données réelles.

> **Note** : pour la **stratégie historique** du choix d'outil (DataFrameModel
> custom plutôt que Pandera, Soda, etc.), voir
> [`data_quality_strategy.md`](data_quality_strategy.md). Pour le **design**
> du `DataFrameModel`, voir
> [`dataframe_model_custom.md`](dataframe_model_custom.md).

Mise à jour : à chaque fois qu'on relâche ou durcit une contrainte sur un
`SilverSchema`, ajouter une entrée dans la section « Décisions notables » avec
date de vérification et données observées.

## Philosophie

### Ce qu'on valide

1. **Schéma structurel** : noms de colonnes attendus + dtypes Polars (via
   `_check_schema`). Toute colonne manquante / inattendue / mal typée fait
   échouer le silver.
2. **Drift source** : les colonnes source post-`prepare_silver` doivent matcher
   `_ALL_SOURCE_COLUMNS` (via `validate_source_columns`). Détecte une
   évolution de l'API amont (colonne ajoutée, retirée, renommée).
3. **Attributs maître `nullable=False`** : identifiants stables, attributs
   calculés (`est_*`, `type_energie`), grandeurs structurelles (`id_station`,
   `code_eic_resource_object`, ...). Une valeur NULL signale une corruption.
4. **Bornes physiques** quand elles sont vraies dans toutes les conditions
   d'opération (ex : `humidite ∈ [0, 100]`, `direction_vent ∈ [0, 360]`,
   `nebulosite ∈ [0, 9]`).
5. **Unicité de clé primaire** : appliquée post-collect via dedup conditionnel
   dans `run_silver` (clé déclarée dans `DatasetTransformSpec.primary_key`).

### Ce qu'on **ne** valide **pas**

1. **Valeurs des champs source** (`code_filiere`, `code_region`,
   `code_insee_region`, `code_departement`, ...) : pas de `isin`. Ces valeurs
   viennent de l'API ; on accepte leur évolution. Le drift de **présence** est
   couvert par `validate_source_columns`.
2. **Bornes "théoriques" qui ne tiennent pas en pratique** : si la donnée
   réelle viole occasionnellement une borne pour des raisons légitimes
   (régularisation comptable, convention de signe, station hors plage), on
   relâche la borne et on documente ici.
3. **Cohérence cross-column** (ex : `rafale_max ≥ vitesse_vent`) : non
   couverte par `DataFrameModel`. Si nécessaire, à implémenter via
   `_warn_*` columns dans le `transform_silver`.

### Stratégie face à une divergence

| Cas | Action |
|---|---|
| Drift de schéma source (colonne ajoutée) | Échec rapide → mettre à jour `_ALL_SOURCE_COLUMNS` après revue |
| Master attr NULL pour un row | Échec dur (`SchemaValidationError`) → investiguer la source |
| Borne physique violée par une valeur claire (capteur HS) | À investiguer ; soit relâcher la borne, soit filtrer en amont |
| Borne violée par convention métier (signe, dépassement légitime) | Relâcher la borne + entrée dans ce document |
| Doublons sur clé primaire | Dedup conditionnel (`keep="last"`) + log info |
| Colonne diagnostique `_diag_*` / `_warn_*` non vide | Loggée automatiquement par `extract_diagnostics` (info / warning) |

### Défense en profondeur : Python ↔ Postgres

Toute contrainte du `SilverSchema` Python doit avoir son miroir dans
`dbt/models/silver/_sources.yml` :
- `nullable=False` → test dbt `not_null`
- `unique=True` → test dbt `unique`
- `isin=[...]` → test dbt `accepted_values`

La cohérence est enforced par `tests/test_dbt_consistency.py` (CI bloque tout
drift).

## Décisions notables

### `meteo_france_climatologie` : `pression_station` non bornée

**Contexte.** `pression_station` mesure la pression à l'altitude de la station,
**pas** réduite au niveau mer. Stations en altitude (Pic du Midi 2877m,
Aiguille du Midi 3842m) ont des pressions ~600-700 hPa, légitimes
physiquement. Bornes initialement proposées `[940, 1060]` ne s'appliquent
qu'à `pression_mer`.

**Décision.** `pression_station` non bornée. Bornes `[940, 1060]` hPa
appliquées uniquement à `pression_mer` (réduite au niveau mer).

**Vérifié le.** 2026-05-04 sur bronze 2026-04-27 : 86 237 valeurs
`pression_station < 940 hPa`.

### `meteo_france_stations` : `lieu_dit` peut être NULL

**Contexte.** L'audit initial supposait `lieu_dit` toujours présent (attribut
maître). En réalité ~8% des stations Météo France n'ont pas de `lieu_dit`
renseigné dans la source (souvent stations spécialisées : aéroports
principaux, stations automatiques en mer, etc.).

**Décision.** `lieu_dit` reste `nullable=True` (défaut). Test `not_null` côté
dbt retiré.

**Vérifié le.** 2026-05-04 sur bronze 2026-04-27 : 201 / 2 392 stations
(8.4%) avec `lieu_dit` NULL.

### `odre_eco2mix_*` : production électrique non bornée à `ge=0`

**Contexte.** Les colonnes de production (`thermique`, `nucleaire`, `eolien`,
`solaire`, `hydraulique`, `bioenergies`, `eolien_terrestre`,
`eolien_offshore`) peuvent être occasionnellement négatives dans les données
ODRE :
- Auto-consommation des auxiliaires (panneaux solaires la nuit, pompes des
  centrales, ...).
- Régularisations comptables RTE rétroactives.
- Faibles pourcentages mais existants (mesuré 0.0003% à 2.9% selon la
  filière).

`pompage`, `stockage_batterie`, `destockage_batterie` suivent une **convention
de signe bidirectionnelle** : négatif = absorption d'énergie (mode pompage
/ stockage), positif = restitution (mode turbine / déstockage). 28% des
valeurs `pompage` sont négatives.

`tco_*` (taux de couverture) et `tch_*` (taux de charge) peuvent dépasser
100 % :
- `tco > 100%` : régions exportatrices (production locale > consommation
  locale).
- `tch > 100%` : décalage temporel entre la mise à jour de la capacité
  installée et le calcul du taux.

**Décision.** Toutes ces colonnes sont **non bornées** sur les deux variantes
(`tr` + `cons_def`). Seule `consommation` reste bornée à `ge=0` (toujours
positive). `ech_physiques` est non bornée par design (peut être négative
pour export).

**Vérifié le.** 2026-05-04 sur bronze 2026-04-27 :
- cons_def : 78 327 valeurs `thermique < 0`, 33 429 `solaire < 0`,
  764 558 `pompage < 0`, jusqu'à 435 452 `tco_nucleaire > 100`.
- tr : 21 034 `pompage < 0`, 41 507 `tco_nucleaire > 100`.

### `odre_installations` : `energie_annuelle_glissante_*` non bornée

**Contexte.** L'énergie annuelle glissante (injectée, produite, soutirée)
peut être occasionnellement négative dans les données ODRE pour cause de
régularisations comptables (corrections rétroactives de mesures).

**Décision.** Les 3 colonnes `energie_annuelle_glissante_*` sont non bornées.
Les autres colonnes physiques (puissances, `productible`, `capacite_reservoir`,
`hauteur_chute`, `debit_maximal`, counts) restent bornées à `ge=0`.

**Vérifié le.** 2026-05-04 sur bronze 2026-04-27 : 16 / 133 998 rows
(0.012%) avec `energie_annuelle_glissante_injectee < 0`.

## Outils & scripts

- `scripts/validate_silver_on_existing_bronze.py` : exécute `run_silver` sur
  les derniers bronze locaux pour chaque dataset et reporte les
  `SchemaValidationError`. À lancer avant tout durcissement de contrainte.
- `tests/test_dbt_consistency.py` : enforce le mirror Python ↔ dbt à chaque
  commit.
- `tests/test_climatologie_columns_consistency.py` : enforce l'alignement
  entre la projection download (`_LANDING_COLUMNS`) et la lecture bronze
  (`_BRONZE_COLUMNS`).
