# Architecture hybride ETL + ELT

## Vue d'ensemble

Le projet utilise une **architecture medallion** (Landing → Bronze → Silver → Gold)
avec une approche **hybride ETL + ELT** :

- **ETL** (Python/Polars/DuckDB) : Sources distantes → Landing → Bronze → Silver (
  fichiers .parquet)
- **Load** (psycopg/COPY) : Silver .parquet → Postgres
- **ELT** (dbt-core) : Silver (Postgres) → Gold (Postgres)

```text
                        ETL (Python/Polars/DuckDB)
Sources → Landing → Bronze → Silver (.parquet)
                                 │
                                 │  LOAD (psycopg/COPY)
                                 ▼
                           Silver (Postgres)
                                 │
                                 │  ELT (dbt-core)
                                 ▼
                            Gold (Postgres)
```

## Justification

### Pourquoi ETL pour Landing → Silver

Les données brutes proviennent de sources hétérogènes (archives 7z, GeoPackage, JSON,
parquet). Le traitement **avant** chargement est justifié car Postgres ne sait pas
nativement lire ces formats. Polars et DuckDB sont adaptés à ces transformations
lourdes (parsing, extraction d'archives, opérations spatiales sur fichiers).

### Pourquoi ELT pour Silver → Gold

Une fois les données normalisées dans Postgres, les transformations gold (jointures,
agrégations, calculs spatiaux) se font plus naturellement en SQL directement dans le
moteur de base de données. Cela permet de tirer parti de PostGIS pour les opérations
spatiales et des optimisations du query planner Postgres.

## Stack technique

| Couche                      | Outil                   | Rôle                                                     |
|-----------------------------|-------------------------|----------------------------------------------------------|
| Landing → Bronze → Silver   | Python, Polars, DuckDB  | ETL : extraction, nettoyage, standardisation             |
| Silver .parquet → Silver PG | psycopg, `COPY`         | Load : chargement bulk dans Postgres                     |
| Silver PG → Gold PG         | dbt-core + dbt-postgres | ELT : transformations SQL dans la base                   |
| Orchestration               | Apache Airflow          | Scheduling et dépendances entre tâches                   |
| Spatial                     | PostGIS                 | Jointures spatiales et calculs de distance (couche gold) |

## dbt-core : structure proposée

```text
dbt/
├── dbt_project.yml
├── profiles.yml              # connexion Postgres (via env vars)
├── models/
│   ├── staging/              # Vues 1:1 sur les tables silver (renommage, casting)
│   │   ├── stg_odre_installations.sql
│   │   ├── stg_meteo_france_stations.sql
│   │   ├── stg_ign_contours_iris.sql
│   │   ├── stg_eco2mix_cons_def.sql
│   │   └── stg_eco2mix_tr.sql
│   └── marts/                # Couche Gold — modèles analytiques
│       └── gold_inst_elec_stations_meteo.sql
├── tests/                    # Tests custom SQL
└── macros/                   # Macros Jinja réutilisables
```

### Couche staging

Vues légères (pas de matérialisation) qui servent d'interface propre sur les tables
silver. Renommage éventuel, casting de types, filtrage de colonnes inutiles.

### Couche marts (gold)

Modèles matérialisés (tables) contenant la logique métier :

- **`gold_inst_elec_stations_meteo`** : jointure spatiale entre installations
  électriques et stations météo via PostGIS (`ST_DWithin`, `ST_Distance`).
  Remplace le calcul Haversine actuellement fait en DuckDB.
- Modèles incrémentaux pour `eco2mix_tr` (données horaires, gros volume).

## Orchestration Airflow

```text
[Extract] → [Bronze] → [Silver parquet] → [Load to PG] → [dbt run] → [dbt test]
     ETL Python existant                    nouveau        ELT dbt
```

Intégration possible via :

- **`cosmos`** (astronomer-cosmos) : opérateur Airflow natif pour dbt
- **`BashOperator`** : alternative simple avec `dbt run` / `dbt test`

## Avantages de l'approche hybride

- **Showcase des deux paradigmes** ETL et ELT dans un même projet
- **Lineage et documentation** automatiques via dbt (DAG de dépendances, docs générées)
- **Tests intégrés** dbt (`unique`, `not_null`, `accepted_values`, tests custom SQL)
- **Modèles incrémentaux** pour les datasets volumineux (eco2mix horaire)
- **Conservation des parquet silver** comme backup/archive indépendant de la base
- **PostGIS** pour les opérations spatiales directement dans le moteur SQL

## Alternatives considérées pour la couche ELT

### dbt-core (choix retenu)

Standard de facto en data engineering. Racheté par Fivetran en octobre 2025.

| Pour                                               | Contre                                                       |
|----------------------------------------------------|--------------------------------------------------------------|
| Reconnu par l'industrie et les recruteurs          | Nouvelle dépendance (dbt-core + dbt-postgres)                |
| Lineage, documentation et tests intégrés           | Courbe d'apprentissage (conventions `ref()`, staging/marts)  |
| Templating Jinja + SQL, macros réutilisables       | Deux paradigmes de transformation à maintenir (Python + SQL) |
| Modèles incrémentaux natifs                        | SQL uniquement (pas de Python dans les modèles)              |
| Intégration Airflow via `cosmos` ou `BashOperator` |                                                              |

### SQLMesh

Framework open source racheté par Fivetran en 2025 (via Tobiko Data). Compatible avec
les projets dbt existants.

| Pour                                        | Contre                                     |
|---------------------------------------------|--------------------------------------------|
| Modèles en Python **ou** SQL                | Moins connu que dbt                        |
| Environnements versionnés (dev/prod) natifs | Écosystème plus restreint                  |
| Détection automatique des changements       | Avenir incertain post-acquisition Fivetran |
| Compatible avec les projets dbt existants   | Moins de ressources/tutoriels disponibles  |

### lea (lightweight ELT alternative)

Framework Python minimaliste de transformation SQL, créé par Max Halford (créateur de
River ML). Approche pragmatique : fichiers SQL simples, gestion des dépendances entre
vues/tables.

| Pour                                        | Contre                                 |
|---------------------------------------------|----------------------------------------|
| Extrêmement léger, pas de magie             | Très peu connu, petit écosystème       |
| Python-native, fichiers SQL simples         | Pas de lineage/docs auto comme dbt     |
| Facile à intégrer dans un pipeline existant | Pas de tests intégrés                  |
|                                             | Projet maintenu par une seule personne |

### SQL pur orchestré par Airflow (sans framework)

Transformations gold écrites en SQL brut, orchestrées directement avec le
`PostgresOperator` d'Airflow.

| Pour                                 | Contre                                   |
|--------------------------------------|------------------------------------------|
| Zéro dépendance supplémentaire       | Pas de lineage, pas de tests intégrés    |
| Contrôle total, simple et explicite  | Réinvente une partie de ce que dbt fait  |
| Airflow déjà en place dans le projet | Maintenance manuelle des dépendances SQL |
|                                      | Moins impressionnant en portfolio        |

### Tout en Python (sans framework ELT)

Stack unifiée Polars/DuckDB pour toutes les couches, y compris gold.

| Pour                         | Contre                                               |
|------------------------------|------------------------------------------------------|
| Pas de nouvelle dépendance   | Ne showcase pas l'approche ELT                       |
| Cohérence de la stack Python | Perd les bénéfices de lineage/tests SQL automatiques |
| DuckDB/Polars déjà maîtrisés | Transformations gold moins naturelles hors de la DB  |

### Note sur dlt (data load tool)

`dlt` n'est **pas** une alternative à dbt. C'est un outil de **Load** (le L de ELT),
pas de transformation. Il pourrait remplacer le step de chargement `psycopg/COPY` dans
l'architecture, mais un framework de transformation (dbt ou équivalent) resterait
nécessaire pour la couche silver → gold. Le combo `dlt` + `dbt` est un pattern courant.

## Classement des alternatives

| Rang | Outil                 | Justification                                                             |
|------|-----------------------|---------------------------------------------------------------------------|
| 1    | **dbt-core**          | Standard industrie, max de valeur portfolio, tests + docs + lineage       |
| 2    | **SQLMesh**           | Alternative moderne si on veut montrer la connaissance des outils récents |
| 3    | **SQL pur + Airflow** | Approche minimaliste, évite une dépendance supplémentaire                 |
