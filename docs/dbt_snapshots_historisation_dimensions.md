# Piste : dbt snapshots pour historiser les dimensions silver

> **Date** : 2026-05-11
> **Statut** : Piste à évaluer (pas implémenté)

## Contexte

Les trois dimensions silver (`dim_installations`, `dim_stations_meteo`,
`dim_contours_iris`) sont **écrasées à chaque run** par le pipeline Python (mode
snapshot complet). On ne conserve donc aucun historique des changements de ligne :
impossible de répondre à *« quelle était la puissance solaire installée fin
2024-Q3 ? »* ou *« quand cette centrale est-elle devenue inactive ? »*.

Les `dbt snapshots` implémentent du SCD Type 2 : pour chaque clé primaire, dbt
détecte les changements de colonnes et matérialise une table avec `dbt_valid_from` /
`dbt_valid_to`, permettant des jointures temporelles avec les faits eco2mix /
climatologie (`date_heure BETWEEN dbt_valid_from AND dbt_valid_to`).

## Bilan par table

| Table                  | Snapshot utile ? | Raison |
|------------------------|------------------|--------|
| `dim_installations`    | **Oui — cas d'école** | `est_actif`, `puis_max_installee`, `type_energie`, `est_renouvelable`, `code_iris` évoluent (mise en service, déclassement, rénovation, reclassification). PK `code_eic_resource_object` déjà unique. |
| `dim_stations_meteo`   | Oui, secondaire  | `mesure_solaire` / `mesure_eolien` / `params_*` peuvent changer (ajout/retrait de capteurs). Une station fermée disparaît du dump suivant → le snapshot la « ferme » correctement. À activer après `dim_installations`. |
| `dim_contours_iris`    | Non              | Référentiel IGN/INSEE quasi-statique (révision tous les 5-10 ans). Mieux géré par une colonne `millesime` que par SCD2. |
| `fact_eco2mix_*`, `fact_meteo_horaire` | Non | Faits déjà historisés par `date_heure` dans la PK. Les snapshots dbt visent les dimensions à PK stable. La transition `eco2mix_tr` → `eco2mix_cons_def` est déjà gérée par deux tables (pattern de versionnage adapté). |

## Esquisse d'implémentation (`dim_installations`)

Arborescence :

```
dbt/
  snapshots/                          ← nouveau dossier
    snap_dim_installations.sql
  models/silver/
    stg_dim_installations.sql         ← reste current state (inchangé)
    stg_dim_installations_scd.sql     ← optionnel : vue sur le snapshot
```

`dbt_project.yml` :

```yaml
snapshot-paths: ["snapshots"]
snapshots:
  data_eng_gold:
    +target_schema: silver_history    # ou silver
    +strategy: check
```

`dbt/snapshots/snap_dim_installations.sql` :

```sql
{% snapshot snap_dim_installations %}
{{ config(
    unique_key='code_eic_resource_object',
    strategy='check',
    check_cols=['est_actif', 'puis_max_installee', 'type_energie',
                'est_renouvelable', 'code_iris', 'code_departement', 'code_region'],
) }}
SELECT * FROM {{ source('silver', 'dim_installations') }}
{% endsnapshot %}
```

Stratégie `check` (pas `timestamp`) : le pipeline Python n'écrit pas de colonne
`updated_at` sur les tables dimensionnelles.

## Points d'attention

1. **Pas de rétro-historique** : l'historique commence le jour de l'activation des
   snapshots. Si on veut conserver l'avant, archiver d'abord les Parquet silver
   versionnés déjà disponibles dans `data/silver/`.
2. **Ordre des tâches Airflow** : `dbt snapshot` doit tourner **avant** que le
   pipeline Python n'écrase `silver.dim_installations`. Séquence cible :
   `silver write` → `dbt snapshot` → `dbt run`. Inverser fait rater les transitions
   intermédiaires (peu critique pour des installations qui changent rarement, mais à
   garder en tête).
3. **Tests dbt** : le snapshot ne peut plus tester `unique` sur
   `code_eic_resource_object` (plusieurs versions par clé). Le test devient
   `unique_combination(code_eic_resource_object, dbt_valid_from)`.
4. **Coût** : négligeable — quelques milliers de lignes, changements rares.

## Alternative écartée (a priori)

Historisation côté ETL en Python (MERGE incrémental avec colonne `valid_from`) :
plus de contrôle mais réimplémente `dbt snapshots`, peu de valeur ajoutée vu que dbt
est déjà dans la stack.
