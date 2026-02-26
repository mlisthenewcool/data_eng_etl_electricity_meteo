# Implémentation dbt dans Airflow

Plan d'implémentation pour intégrer dbt-core dans le pipeline Airflow existant.
L'objectif est de transformer les données silver (Postgres) en données gold
(Postgres) via des modèles dbt orchestrés par Airflow.

## Architecture cible

```text
remote_dataset_factory.py          load_pg_factory.py           dbt_transform_dag.py
┌─────────────────────┐           ┌──────────────────┐         ┌──────────────────┐
│ ingest_odre_instal. │──Asset──→ │ load_odre_instal. │──Asset─→│                  │
│ ingest_meteo_stat.  │──Asset──→ │ load_meteo_stat.  │──Asset─→│  dbt run         │
│ ingest_ign_contours │──Asset──→ │ load_ign_contours │──Asset─→│  dbt test        │
│ ingest_eco2mix_*    │──Asset──→ │ load_eco2mix_*    │         │                  │
└─────────────────────┘           └──────────────────┘         └──────────────────┘
   silver .parquet                   silver PG                    gold PG
   (1 DAG/dataset)                   (1 DAG/dataset)              (1 DAG, AssetAll)
   EXISTANT                          À IMPLÉMENTER                À IMPLÉMENTER
```

## Chaînage des DAGs par Assets

Trois niveaux de DAGs communiquent via le mécanisme d'Assets Airflow :

### Niveau 1 — Ingestion (existant)

Les DAGs `ingest_*` produisent des Assets silver fichier en outlet. Déjà en place
dans `remote_dataset_factory.py`.

### Niveau 2 — Load Postgres (à implémenter)

Un DAG par dataset, généré par un `load_pg_factory.py`. Chaque DAG :

- **Inlet** : Asset silver fichier (`odre_installations__silver`, etc.)
- **Action** : charge le parquet silver dans la table Postgres correspondante
- **Outlet** : Asset silver PG (`odre_installations__silver_pg`, etc.)

Le DAG se déclenche automatiquement quand l'Asset silver fichier est mis à jour.
Si le pipeline d'ingestion short-circuit (données inchangées), le load ne se
déclenche pas.

### Niveau 3 — dbt (à implémenter)

Un seul DAG `dbt_silver_to_gold` :

- **Inlet** : plusieurs Assets silver PG via `AssetAll(...)` — le DAG ne se lance
  que quand **tous** les Assets PG requis ont été mis à jour
- **Action** : `dbt run` puis `dbt test`
- **Outlet** : Asset(s) gold PG

```python
from airflow.sdk import AssetAll

schedule = AssetAll(
    asset_silver_pg_installations,
    asset_silver_pg_stations,
    asset_silver_pg_contours,
)
```

### Note sur les Assets PG

Les Assets PG sont des **marqueurs logiques** identifiés par une URI conventionnelle
(ex: `postgres://project/silver.odre_installations`). Airflow ne vérifie pas le
contenu à l'URI : c'est le task qui émet l'événement outlet en fin d'exécution,
signalant que la donnée a été mise à jour. C'est le même mécanisme que pour les
Assets silver fichier déjà en place.

La garantie de changement réel repose sur la chaîne de short-circuits en amont :
pas de changement source → pas d'événement silver → pas de load → pas d'événement
PG → pas de dbt.

## Prérequis

### PostGIS

L'extension PostGIS est nécessaire dans la base `project` pour les jointures
spatiales dans les modèles gold (remplacement des calculs Haversine DuckDB).

Options :

- Utiliser l'image `postgis/postgis:17-3.5` au lieu de `postgres:17` dans
  `docker-compose.yaml`
- Ou installer l'extension manuellement dans le script d'init

### Schémas Postgres

Créer les schémas `silver` et `gold` dans la base `project` via un script d'init
(`postgres/init/`) :

```sql
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

## Options d'intégration de dbt

### Option 1 — dbt dans le conteneur Airflow (recommandée)

Installer `dbt-core` et `dbt-postgres` directement dans l'image Airflow existante.
Le dossier `dbt/` est monté en bind volume. Airflow lance dbt via `BashOperator`
ou `@task` avec `subprocess`.

**C'est l'option détaillée ci-dessous.**

### Option 2 — Conteneur dbt dédié

Un service Docker séparé pour dbt, déclenché par Airflow via `DockerOperator` ou
`docker compose run`. Avantage : séparation propre (un conteneur = un rôle), image
légère dédiée. Inconvénient : orchestration plus complexe depuis Airflow.

### Option 3 — astronomer-cosmos

La librairie `cosmos` parse le projet dbt et génère automatiquement un task Airflow
par modèle dbt, avec les dépendances `ref()` câblées. Avantage : granularité
maximale dans l'UI (retry par modèle, logs par modèle). Inconvénient : dépendance
supplémentaire, over-engineering pour un petit nombre de modèles (5-6). Pertinent
à partir de ~20 modèles dbt.

## Détail de l'option 1

### Modifications Docker

#### `airflow.Dockerfile`

Ajouter `dbt-core` et `dbt-postgres` au `uv pip install` :

```dockerfile
RUN uv pip install --no-cache \
    duckdb "httpx[http2]" orjson polars "psycopg[c]" py7zr pyarrow \
    pydantic pydantic-settings pyaml structlog tqdm \
    dbt-core dbt-postgres
```

#### `docker-compose.yaml`

Ajouter le bind mount du dossier dbt dans le service `airflow_service` :

```yaml
volumes:
  - ./dbt:/opt/airflow/dbt
```

### Structure du projet dbt

```text
dbt/
├── dbt_project.yml
├── profiles.yml              # connexion Postgres via env vars
├── models/
│   ├── staging/              # vues 1:1 sur les tables silver PG
│   │   ├── _staging.yml      # schema + tests (unique, not_null, ...)
│   │   ├── stg_odre_installations.sql
│   │   ├── stg_meteo_france_stations.sql
│   │   ├── stg_ign_contours_iris.sql
│   │   ├── stg_eco2mix_cons_def.sql
│   │   └── stg_eco2mix_tr.sql
│   └── marts/                # couche gold — modèles matérialisés
│       ├── _marts.yml        # schema + tests
│       └── gold_inst_elec_stations_meteo.sql
├── tests/                    # tests custom SQL
└── macros/                   # macros Jinja réutilisables
```

#### `profiles.yml`

```yaml
data_eng:
  target: docker
  outputs:
    docker:
      type: postgres
      host: postgres_service    # nom du service Docker Compose
      port: 5432
      dbname: "{{ env_var('POSTGRES_DB_NAME') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      schema: gold
      threads: 2
```

Note : les secrets Postgres sont actuellement gérés via Docker secrets
(`/run/secrets/...`). Il faudra soit passer les credentials en variables
d'environnement pour dbt, soit lire les fichiers secrets dans un wrapper script.

#### Couche staging

Vues (non matérialisées) qui servent d'interface propre sur les tables silver PG.
Renommage éventuel, casting de types, filtrage de colonnes inutiles.

```sql
-- models/staging/stg_odre_installations.sql
{{ config(materialized='view', schema='silver') }}

select *
from {{ source('silver', 'odre_installations') }}
```

#### Couche marts (gold)

Modèles matérialisés (tables) contenant la logique métier. Exemple : jointure
spatiale entre installations électriques et stations météo via PostGIS
(`ST_DWithin`, `ST_Distance`), remplaçant le calcul Haversine DuckDB actuel
(`gold_inst_elec_stations_meteo.py`).

Modèles incrémentaux envisageables pour `eco2mix_tr` (données horaires, gros
volume).

### Nouveau DAG factory : `load_pg_factory.py`

Fichier à créer dans `airflow/dags/`. Génère un DAG par dataset silver du
catalogue. Chaque DAG :

1. Lit le parquet silver (`silver/{dataset}/current.parquet`)
2. Charge les données dans `silver.{dataset_name}` via `psycopg` + `COPY`
3. Émet un outlet Asset PG

### Nouveau DAG : `dbt_transform_dag.py`

Fichier à créer dans `airflow/dags/`. Un seul DAG schedulé sur `AssetAll(...)` des
Assets silver PG :

```python
@task(task_id="dbt_run")
def dbt_run():
    subprocess.run(
        ["dbt", "run", "--project-dir", "/opt/airflow/dbt", "--profiles-dir",
         "/opt/airflow/dbt"],
        check=True,
    )


@task(task_id="dbt_test")
def dbt_test():
    subprocess.run(
        ["dbt", "test", "--project-dir", "/opt/airflow/dbt", "--profiles-dir",
         "/opt/airflow/dbt"],
        check=True,
    )
```

### Fichiers à créer/modifier

| Fichier                               | Action   | Description                                                |
|---------------------------------------|----------|------------------------------------------------------------|
| `airflow.Dockerfile`                  | Modifier | Ajouter `dbt-core dbt-postgres`                            |
| `docker-compose.yaml`                 | Modifier | Ajouter bind mount `./dbt:/opt/airflow/dbt`, image PostGIS |
| `postgres/init/02_create_schemas.sql` | Créer    | Schémas `silver` et `gold`                                 |
| `dbt/`                                | Créer    | Projet dbt complet (profiles, models, tests)               |
| `airflow/dags/load_pg_factory.py`     | Créer    | DAG factory pour le load silver → PG                       |
| `airflow/dags/dbt_transform_dag.py`   | Créer    | DAG pour `dbt run` + `dbt test`                            |
| `airflow/assets.py`                   | Modifier | Ajouter factory pour Assets silver PG                      |
