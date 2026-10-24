# Prototype V2 (temporaire)

Éléments validés pendant l'étude de la V2 (24 octobre 2026), conservés pour servir de
base à la migration décrite dans [`docs/migration_v2.md`](../docs/migration_v2.md).

**Ce dossier est voué à disparaître** : chaque élément est absorbé par le vrai projet au
fil des étapes (modèles dbt dans `dbt/`, scripts d'infrastructure, tests). Il est hors
du périmètre de ruff, ty et du formateur de docstrings (hooks limités à `notebooks/`,
`scripts/`, `src/`, `tests/`).

## Contenu

| Fichier | Rôle | Statut |
|---|---|---|
| `silver_sql.py` | Traduction SQL DuckDB des 6 transformations silver et du modèle gold | **validée** : identique ligne à ligne à la sortie Polars (6 datasets) et au gold PostGIS/dbt |
| `run_silver_duckdb.py` | Exécution de cette traduction en DuckDB pur sur des fichiers bronze locaux (équivalent de `run_silver`) | validé |
| `reference_polars.py` | Production des sorties silver de référence avec le code Polars actuel | à lancer **avant** de supprimer Polars |
| `verify_equality.py` | Égalité ligne à ligne entre une référence et un candidat (Parquet, table DuckLake ou table Postgres) | validé ; critère d'acceptation de chaque étape |
| `lake_admin.py` | Création du lac DuckLake et de ses schémas, maintenance (snapshots, fichiers orphelins, compaction) | validé (création, expiration, purge) ; compaction non testée |
| `dbt_spike/` | Projet dbt v2 de l'essai : bronze, silver (lambdas, spatial, clé synthétique, incrémental `merge`), contrat, tests, test unitaire, gold, mart publié dans Postgres | validé avec dbt-oss 2.0.8 : 16 builds verts sur 16 |

## Limites connues

- **Bronze du prototype** : `silver_sql.py` part des fichiers bronze **actuels** (produits
  par les `transform_bronze` Polars). La traduction landing → bronze n'est pas encore
  faite ni validée (climatologie : valeurs sentinelles `""` et `mq` → NULL puis cast
  strict ; stations : `read_json` ; IRIS : `ST_Read` du GeoPackage + `ST_AsWKB`).
- **`dbt_spike/` matérialise le bronze en tables** (copie à chaque run). La stratégie
  retenue est d'en faire des **vues** sur `landing/`, pour ne pas faire gonfler le lac.
- **`dbt_spike/` ne couvre qu'une partie des colonnes** de chaque dataset ; la version
  complète est dans `silver_sql.py`.
- Les diagnostics `_diag_` / `_warn_` ne sont pas traduits (futurs tests dbt en
  `severity: warn`).

## Utilisation

```bash
# 1. Référence Polars (environnement du projet)
uv run python prototype_v2/reference_polars.py <bronze_dir> <ref_dir>

# 2. Traduction DuckDB
python prototype_v2/run_silver_duckdb.py <bronze_dir> <ref_dir> <out_dir>

# 3. Égalité, dataset par dataset
python prototype_v2/verify_equality.py "'<ref_dir>/odre_installations.parquet'" \
    "'<out_dir>/odre_installations.parquet'"

# 4. Projet dbt (dbt-oss + driver DuckDB installé par `dbc install duckdb`)
export S3_ENDPOINT=... S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... S3_REGION=...
export LAKE_CATALOG="postgres:dbname=lake_catalog host=... user=... password=..."
export LAKE_DATA_PATH="s3://lake/lake/" PG_DSN="dbname=... host=... user=... password=..."
python prototype_v2/lake_admin.py init main_bronze main_silver main_gold main_silver_dbt_test__audit
cd prototype_v2/dbt_spike && dbt build --profiles-dir . --vars '{bronze_path: "s3://lake/landing"}'
```
