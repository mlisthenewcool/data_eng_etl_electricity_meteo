# Migration V2 : dbt + DuckDB sur S3, Postgres pour servir

> **Statut (2026-10-24) : décidé, migration à démarrer.** Ce document fixe les choix,
> le plan par étapes et la méthode de validation. Les éléments de prototype validés
> sont dans [`prototype_v2/`](../prototype_v2/README.md).

## Pourquoi

La stack actuelle contrôle la qualité à trois endroits (Polars `DataFrameModel`,
contraintes Postgres, tests dbt), mélange trois moteurs de calcul (Polars, DuckDB,
Postgres) et passe l'essentiel de son temps à charger le silver dans Postgres.

Mesures sur les 6 datasets (bronze identique, même machine, 16 cœurs, 31 Go) :

| | Stack actuelle | Stack V2 | Gain |
|---|---|---|---|
| Chargement complet | 362 s (dont 344 s de chargement Postgres) | 11,7 s | × 31 |
| Run quotidien | ≈ 59 s | ≈ 12 s | × 5 |
| Pic mémoire du pipeline | 7,9 Go (Polars) | 2,7 Go (DuckDB plafonné à 3 Go) | ÷ 3 |
| Disque hors bronze | 6,97 Go (dont 6,5 Go dans Postgres) | 0,61 Go | ÷ 11 |
| Requête front lourde (température journalière, 28,8 M lignes) | 2 298 ms | ≤ 9 ms (mart pré-agrégé, tableau de bord entier) | × 250 |
| Code de transformation | 2 239 lignes Python | ≈ 170 lignes SQL | ÷ 13 |
| Sorties silver (6 datasets) et gold | référence | **identiques ligne à ligne** | = |

La stack V2 a été mesurée avec le SQL de `prototype_v2/silver_sql.py` exécuté par DuckDB.
Un prototype dbt a ensuite donné des sorties identiques entre dbt v1 et dbt v2, la v2
étant environ 15 % plus rapide (8,2 s contre 9,8 s à 4 threads, sur une partie des
modèles).

## Stack visée

```
 Sources HTTP (IGN, ODRE, data.gouv, Météo France)
      │
      ▼
┌──────────────── Airflow 3 (Python 3.14)  →  Dagster plus tard ────────────────────────┐
│ 1. Ingestion Python : httpx, py7zr, smart-skip ──boto3──▶ S3 landing/ (fichiers bruts) │
│ 2. dbt v2 (dbt-oss) + DuckDB 2.0 :                                                      │
│      bronze (vues sur landing) → silver (incrémental MERGE) → gold ──▶ S3 lake/         │
│ 3. Publication des marts platinium ──ATTACH postgres──▶ Postgres                        │
│ 4. Maintenance DuckLake : expiration des snapshots, purge des fichiers, compaction      │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        │                                    │
        ▼                                    ▼
┌──── S3 ───────────────────────────┐  ┌──── Postgres 18 (une instance) ─────────────────┐
│ RustFS en local et en CI          │  │ base Airflow                                     │
│ stockage objet managé en cloud    │  │ catalogue DuckLake                               │
│ landing/ : cycle de vie N versions│  │ ops : logs, métriques, état du smart-skip        │
│ lake/    : Parquet DuckLake       │◀─│ platinium : marts exposés au front               │
└───────────────────────────────────┘  └──────────────────────┬───────────────────────────┘
                                                                ▼
                                                             Grafana
```

## Choix arrêtés

| Sujet | Choix | Raison principale |
|---|---|---|
| Python | 3.14 | pas d'image Airflow 3.15 (apache/airflow#74555) |
| Orchestrateur | Airflow 3 | `dagster-dbt` n'accepte pas encore Python 3.14 (dagster-io/dagster#34200) ; Dagster à réévaluer ensuite |
| Ingestion | httpx, py7zr, smart-skip ; dépôt dans S3 avec **boto3** (+ `boto3-stubs[s3]`) | standard du marché ; DuckDB ne sait pas copier des fichiers binaires bruts |
| Transformations | **dbt v2 (dbt-oss 2.x, Apache-2.0)** + **DuckDB 2.0**, driver DuckDB installé par `dbc` | open source, sorties identiques à dbt v1, plus rapide, retire dbt-core des dépendances Python de l'image Airflow |
| Stockage du lac | **DuckLake** : Parquet sur S3, catalogue dans Postgres | ACID, snapshots (remplacent la rotation `current`/`backup`), `MERGE` (remplace le diff Polars) |
| Serveur S3 | **RustFS** en local et en CI ; stockage objet managé en cloud | ne sert qu'au dev et à la CI ; même code partout, seule l'adresse change |
| Postgres | orchestrateur, catalogue DuckLake, schéma `ops`, schéma `platinium` | plus aucune donnée bronze ou silver dans Postgres |
| Front | **Grafana** sur les marts `platinium` | Postgres plus rapide et plus sobre que DuckDB-Parquet et ClickHouse sur des marts agrégés (1, 10, 50 utilisateurs) |
| Tests | **moto** (logique d'ingestion), conteneur **RustFS** (intégration DuckDB, DuckLake, dbt) | moto ne couvre que boto3 |
| Écartés | Polars, ClickHouse, pg_loader, `DataFrameModel`, DDL Postgres du silver | voir les mesures ci-dessus |

## Contournements dbt v2 (temporaires)

dbt-oss 2.0.8 n'applique la configuration du profil qu'à la première de ses connexions
(PR en brouillon dbt-labs/dbt#16567). Conséquences et parades :

| Problème | Parade | À retirer quand |
|---|---|---|
| `settings:` du profil ignorés | ne jamais compter dessus ; réglages DuckDB par `SET GLOBAL` en `pre-hook` ; un test dbt vérifie les réglages actifs pendant le build | correction de #16567 |
| `geometry_always_xy` (exigé par DuckDB 2.0 pour `ST_Distance_Sphere`) | `+pre-hook: "SET GLOBAL geometry_always_xy = true"` au niveau du projet | DuckDB 2.1 (valeur par défaut) |
| Option `data_path` de l'attach DuckLake non transmise | lac créé par le script d'init | correction de #16567 (bonne pratique de toute façon) |
| Schémas DuckLake créés en parallèle invisibles entre threads (échecs intermittents de commit) | schémas créés par le script d'init + **test CI** de cohérence avec `dbt_project.yml` | correction de #16567 |
| Identifiants S3 dans `settings:` ignorés | bloc `secrets:` du profil | jamais (méthode recommandée par DuckDB) |

Ces contournements représentent moins de 60 lignes. En cas d'oubli d'un schéma, la panne
est bruyante (échec de la tâche), sans écriture partielle (DuckLake est transactionnel),
et se répare au retry. Repli prouvé : le même SQL donne les mêmes sorties avec
dbt-core 1.12 + dbt-duckdb 1.11, sans contournement.

## Maîtrise du volume

DuckLake n'écrit jamais en place : chaque écriture crée de nouveaux fichiers Parquet,
conservés jusqu'à la maintenance (12 reconstructions complètes du prototype ont fait
monter le lac à 5,9 Go).

| Couche | Stratégie |
|---|---|
| Landing | smart-skip + règle de cycle de vie S3 (N dernières versions) |
| Bronze | **vues** sur `landing/` (conversion de format à la lecture), pas de copie |
| Silver climatologie, eco2mix | modèle incrémental `merge` sur la clé primaire (≈ 44 000 lignes par jour pour climatologie) |
| Silver référentiels, gold, platinium | reconstruction complète (quelques Mo) |
| Maintenance | après chaque build : `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_merge_adjacent_files` (rétention à fixer, 7 jours par défaut) |

Estimation (non mesurée) : lac sous 1 Go, plus la rétention de `landing/`. Pour mémoire,
`data/bronze/` pèse aujourd'hui 12 Go (toutes les versions sont conservées).

## Plan de migration

Une PR par étape ; la stack actuelle reste fonctionnelle à chaque étape.

| # | Branche | Contenu | Critère d'acceptation |
|---|---|---|---|
| 1 | `feat/v2-infra` | RustFS dans `docker-compose` ; init : bucket, base `lake_catalog`, lac et schémas, schémas `ops` et `platinium` ; `dbc` + driver DuckDB et `dbt-oss` dans l'image ; variables S3 dans `settings.py` et `.env.example` | `docker compose up` crée tout ; un `dbt debug` passe dans le conteneur |
| 2 | `feat/v2-dbt-climatologie` | profil dbt DuckDB (lac + Postgres attachés) ; bronze (vue) et silver climatologie en dbt ; contrats et tests à la place du `DataFrameModel` ; test CI de cohérence des schémas | `verify_equality.py` : identique à la référence Polars |
| 3 | `feat/v2-dbt-datasets` | les 5 autres datasets, traduction landing → bronze comprise ; macro dbt équivalente à `prepare_silver` | identique à la référence pour chaque dataset |
| 4 | `feat/v2-dbt-gold` | gold en DuckDB, marts `platinium` publiés dans Postgres | gold identique au gold PostGIS actuel |
| 5 | `feat/v2-ingestion-s3` | ingestion vers `landing/` avec boto3 ; état du smart-skip dans `ops` (remplace `data/_state`) ; tests moto | tests unitaires et d'intégration verts |
| 6 | `feat/v2-dags` | DAGs : ingestion → `dbt build` → maintenance DuckLake ; un run actif à la fois | run complet de bout en bout dans Airflow |
| 7 | `chore/v2-cleanup` | suppression de Polars, `pg_loader`, `DataFrameModel`, DDL et upserts du silver, tests associés, `prototype_v2/` ; docs et README | plus aucune dépendance Polars ; CI verte |
| 8 | `feat/v2-grafana` | Grafana dans `docker-compose`, tableaux de bord provisionnés | tableaux de bord sur les marts `platinium` |

**Méthode de validation** (étapes 2 à 4) : produire la référence avec le code actuel
(`prototype_v2/reference_polars.py`) **avant** de supprimer Polars, puis comparer chaque
table V2 avec `prototype_v2/verify_equality.py` (égalité ligne à ligne, dans les deux
sens, session en UTC).

## Pièges vérifiés

- **DuckDB 2.0** refuse l'ancienne syntaxe des lambdas `x -> …` : écrire `lambda x: …`.
- **Ordre des lignes** : le dédoublonnage « garder la dernière » et les clés synthétiques
  des installations dépendent de l'ordre source, implicite en Polars. En SQL, le rendre
  explicite avec `read_parquet(..., file_row_number = true)`.
- **Comparaisons d'horodatages** : une session DuckDB hors UTC convertit les
  `TIMESTAMPTZ` en heure locale et crée de faux écarts.
- **Versions de DuckDB** : épingler la même version pour le driver dbt (`dbc`) et la
  bibliothèque Python ; un écart change le format du catalogue DuckLake (1.0 contre 1.1),
  comme constaté avec le plugin Grafana DuckDB (DuckDB 1.5.6 embarqué).
- **uv ignore les bornes hautes de `Requires-Python`** : un environnement Python 3.15
  installerait silencieusement Airflow et Dagster non supportés.
- **boto3** : régler `request_checksum_calculation` et `response_checksum_validation`
  sur `when_required` pour les serveurs compatibles S3.
- **Deux `dbt build` simultanés sur le même lac** : à éviter (un run actif à la fois).

## Points ouverts

- Correction de dbt-labs/dbt#16567 (retrait des contournements).
- `dagster-dbt` sous Python 3.14 (bascule vers Dagster).
- Image Airflow Python 3.15.
- Durée de rétention des snapshots DuckLake et des versions de `landing/`.
