<!-- ===== Conventions de formatage =====
  - Sections       : Maintenant / Ensuite / Plus tard / Terminé
  - Format ouvert  : - [ ] [Domaine] Description (forme impérative)
  - Format terminé : - [x] [Domaine] _(YYYY-MM-DD)_ Description
  - Tri ouvert     : alphabétique par domaine, puis par description
  - Tri terminé    : date décroissante (plus récent en premier)
  ===== -->

## Maintenant

- [ ] [Données] Ajouter les transformations qui permettent de déterminer les stations
  météo à requêter (meilleure station à moins de 20 km par exemple) et créer le nouveau
  dataset correspondant qui permettra de requêter l'API de Météo France
- [ ] [Pipeline] Évaluer la cohérence entre les contrôles de qualité de données pour la
  couche silver manipulée par plusieurs outils : Polars, Postgres et dbt. Reprendre
  l'architecture si nécessaire et simplifier pour avoir une seule source de référence
- [ ] [Tests] Ajouter des tests sur les modules et fonctions critiques et valider ceux
  déjà créés

## Ensuite

- [ ] [Airflow] Lancer DAGs Asset-scheduled si changements OU si les Assets qui en
  dépendent sont disponibles, mais celui généré par le DAG lui-même n'est pas présent
  (globalement lors du premier lancement si le DAG n'était pas activé, ou autre chose ?)
- [ ] [Données] Intégrer les appels aux API Météo France (phases 2/3 décrites dans
  [integration_meteo_france.md](docs/integration_meteo_france.md))
- [ ] [Docs] Documenter pourquoi et comment le silver est maintenu à l'identique entre
  Postgres et le stockage local
- [ ] [Docs] Rédiger la documentation minimale du projet V1 (présentation, données,
  architecture, outils) —
  [exemple](https://github.com/abeltavares/batch-data-pipeline)
- [ ] [Pipeline] Ajouter un smart skip pour les transformations silver -> gold :
  exécution uniquement si les données ont réellement changé (dans le cas d'un run manuel
  sous Airflow ou une exécution en cli). Suivre la même logique que celle existante.
- [ ] [Pipeline] Définir comment intégrer et gérer l'ajout de nouvelles transformations
  silver (impact sur les données existantes)
- [ ] [Pipeline] Étudier la possibilité d'ajouter une barre de progression comme tqdm
  pour le load dans Postgres
- [ ] [Pipeline] Gérer les cas où les données sont là, mais pas les métadonnées : plutôt
  demander à l'utilisateur de choisir au lieu de relancer tout le téléchargement des
  données + tout le pipeline
- [ ] [Pipeline] Permettre à l'utilisateur de choisir une action lors d'une
  incohérence (par exemple celle de l'état incohérent entre métadonnées et fichiers).
  À faire pour Airflow et en local avec confirmation cli
- [ ] [Postgres] Ajouter extension DuckDB : https://github.com/duckdb/pg_duckdb
- [ ] [Postgres] Résoudre l'accès concurrent au load dans Postgres
- [ ] [Tests] Ajouter des tests de qualité des données post-load (assertions Polars ou
  SQL : nulls, distribution, cohérence temporelle)
- [ ] [Validation] Ajouter un check `no_all_nulls` dans les transformations silver pour
  détecter les régressions silencieuses de l'API source (colonnes critiques entièrement
  NULL). Implémentable dans `shared.py` ou comme méthode de `DataFrameModel`
- [ ] [Validation] Ajouter des métriques de validation dans les logs silver (ex :
  `mesure_solaire_count`/`mesure_eolien_count` pour stations,
  `renouvelables`/`actifs` pour installations)

## Plus tard

- [ ] [Airflow] Créer un DAG de maintenance (hebdomadaire) : nettoyage des fichiers
  bronze obsolètes (`cleanup_old_bronze_versions` existe, mais n'est appelé nulle
  part), validation de la cohérence état/disque (silver file existe ↔ métadonnées
  Airflow existent, inspiré de l'ancien `validator.py`), et audit/nettoyage des
  fichiers state JSON si la persistance locale est implémentée. Implémentation de
  référence dans `todo/dag_maintenance_bronze_retention.py`
  (imports à adapter)
- [ ] [Airflow] Envisager un pattern « error DAG » pour rendre les erreurs de DAG
  factory visibles dans l'UI Airflow (tags `error`/`needs-attention`, task qui raise
  l'erreur) au lieu de simplement logger + skip. Utile si les logs scheduler ne sont
  pas surveillés activement
- [ ] [Airflow] Explorer `@cleanup`, `@teardown`, `task.map` pour améliorer les DAGs
- [ ] [Airflow] Gérer l'incohérence provoquée par le trigger event Asset dans l'UI
- [ ] [Airflow] Intégrer OpenLineage, OpenTelemetry, alertes et SLA
- [ ] [Airflow] Simplifier le nommage des fichiers générés en `frequency.HOURLY`
  (ne pas inclure les minutes et secondes)
- [ ] [Airflow] Trouver comment utiliser `catchup` dans les DAGs (là où il y a un
  `TODO[prod]` dans le code). Plus compliqué qu'il n'y parait, certains DAG auraient
  peut-être besoin d'un catchup (si par exemple 1 run DAG = récupérer les données en
  cours seulement). Si le DAG récupère toutes les données, c'est inutile.
- [ ] [Airflow] Uniformiser les logs (scheduler, triggerer, dag-processor, api-server,
  standalone)
- [ ] [Benchmark] Ajouter des benchmarks de performance (variables des settings,
  traitements, insertion en base)
- [ ] [CI] Ajouter Bandit (analyse de sécurité statique Python) :
  https://github.com/PyCQA/bandit
- [ ] [Docker] Permettre l'utilisation de variables d'environnement pour configurer les
  ports du service Airflow plutôt que des valeurs fixes
- [ ] [Données] Enrichir `catalog.yaml` avec des métadonnées supplémentaires par
  dataset : `quality` (règles de qualité), `schema` (schéma attendu), `tags`,
  `owner`, `source.license`, `source.documentation_url`, `ingestion.retention_days`,
  `ingestion.retention_versions`, `ingestion.filters` (paramètres de requête source)
- [ ] [Données] Implémenter le delta fetch pour `odre_eco2mix_tr` (upsert SQL prêt,
  manque le delta côté source ; séparer les flux def/cons/tr)
- [ ] [Docs] Documenter comment `export AIRFLOW_..._POSTGRES` fonctionne côté Python
- [ ] [Docs] Documenter les variables d'environnement qui configurent le logging
- [ ] [Logging] Ajouter toutes les exceptions custom dans le smoke test visuel
- [ ] [Optimisation] Compression Parquet bronze/silver
- [ ] [Pipeline] Ajouter les options `--skip-download`, `--skip-bronze`,
  `--skip-silver`, `--skip-postgres` aux entrypoints CLI + rétention configurable des
  fichiers landing
- [ ] [Postgres] Définir une stratégie de migration de schémas en production
- [ ] [Postgres] Implémenter l'évolution de schéma automatique
  (`ALTER TABLE ADD COLUMN`) quand le Parquet silver contient de nouvelles colonnes
  absentes de la table PG
- [ ] [Postgres] Uniformiser l'initialisation des bases, schémas et tables (sans
  Airflow)
- [ ] [Prod] Configurer les rôles et privilèges Postgres pour la production
- [ ] [Stockage] Évaluer l'archivage long terme S3 Glacier après période de rétention

## Terminé

- [x] [Pipeline] _(2026-03-09)_ Correction OOM `unique()` silver : dedup conditionnel
  via guard `is_duplicated().any()` dans `run_silver()`, `primary_key` dans
  `DatasetTransformSpec`, suppression de `deduplicate_on_composite_key()`.
  Documenté dans `docs/oom_unique_silver.md`
- [x] [Airflow] _(2026-03-09)_ Refactoring logging dbt : dataclass `_DbtResult` avec
  parsing typé, `chain()` pour les dépendances de tasks
- [x] [Pipeline] _(2026-03-09)_ `is_healing` centralisé dans `PipelineContext`,
  propagé aux smart-skips downstream (extraction incluse)
- [x] [Logging] _(2026-03-09)_ Suppression des logs dupliqués dans `download.py` et
  `extraction.py` (conformité « no duplicate logs »), `shorten_url` rendu public
- [x] [Pipeline] _(2026-03-09)_ Vérification de la cohérence des types entre Polars et
  Postgres dans `pg_loader.py` (`_validate_columns` + `_POLARS_TO_PG_COMPATIBLE`)
- [x] [Données] _(2026-03-09)_ Remplacement de la clé primaire `id_peps` par
  `code_eic_resource_object` dans le catalog, Postgres, dbt et les transformations
- [x] [Pipeline] _(2026-03-09)_ Gestion d'erreurs granulaire (un `except` par
  opération dans `remote_ingestion.py`), messages spécifiques et actionnables
  dans `pg_loader.py`, métriques `duration_s` sur toutes les étapes du pipeline
- [x] [Pipeline] _(2026-03-09)_ `RemoteIngestionPipeline` frozen avec attributs
  privés, suppression du template Jinja Airflow au profit de
  `get_current_context()`, `collect_narrow()` dans `utils/polars.py`
- [x] [Pipeline] _(2026-03-09)_ Optimisation du diff incrémental silver (une seule
  passe d'inner join au lieu de deux scans)
- [x] [Airflow] _(2026-03-09)_ Logging dbt enrichi (résultats model/test avec
  progression, timing, rows affected) et `open_airflow_connection()` encapsulant
  la création du hook
- [x] [Tests] _(2026-03-09)_ Nouveaux tests utils (download, extraction, file_hash,
  progress, remote_metadata), shared et amélioration tests logger
- [x] [Logging] _(2026-03-09)_ Messages d'erreur plus spécifiques et actionnables,
  `_pad_event` comme processeur partagé (console + Airflow), padding augmenté
  à 60 caractères
- [x] [Pipeline] _(2026-03-08)_ Smart-skip pour la climatologie via métadonnées
  data.gouv.fr (`custom_metadata/datagouv.py`, `last_update` au niveau dataset)
- [x] [Transformations] _(2026-03-08)_ Pipeline silver entièrement lazy
  (`LazyFrame` bout en bout, `scan_parquet` → transforms → `.collect()` unique),
  réduction mémoire de ~5 GB à ~2 GB pour la climatologie (18M lignes)
- [x] [Transformations] _(2026-03-08)_ Réorganiser les modules de transformation
  dans `transformations/datasets/`, extraire la logique commune eco2mix dans
  `odre_eco2mix_common.py`
- [x] [CI] _(2026-03-08)_ Ajouter les règles ruff `ANN` (flake8-annotations) et
  corriger les annotations de types dans tout le codebase (`psycopg.Connection`,
  `frozenset`, `TypedDict`, signatures `*`)
- [x] [Misc] _(2026-03-07)_ Résoudre tous les TODO dans le code
- [x] [Logging] _(2026-03-07)_ Uniformiser les logs (messages, niveaux, noms de
  loggers) et le logging entre exceptions custom et les autres modules
- [x] [Pipeline] _(2026-03-07)_ Unifier toutes les timezones sous UTC (Airflow,
  CLI, données, logs) et corriger l'incohérence `Europe/Paris` dans le Dockerfile
- [x] [Pipeline] _(2026-03-07)_ Implémenter la persistance locale du state (JSON
  par dataset dans `data/_state/`) pour le smart-skip hors Airflow
  (`pipeline/state.py`, `cli/runner.py`)
- [x] [Pipeline] _(2026-03-06)_ Ajouter le suivi de progression batch (Airflow)
  pour les téléchargements custom, uniformiser les champs de log (`{subject}_count`,
  `file_size_mib`) et améliorer le logging d'erreurs meteo_download
- [x] [Données] _(2026-03-05)_ Améliorer les transformations : colonnes attendues
  explicites (`EXPECTED_SOURCE_COLUMNS`), `DataFrameModel` avec schémas typés,
  DuckDB justifié pour les centroids IGN (ST_Transform), calculs géographiques
  optimisés
- [x] [Data quality] _(2026-03-05)_ Évaluer les solutions de validation de schémas
  DataFrame (Pandera, Patito, GE, custom) — documenté dans
  `docs/data_quality_strategy.md` et `docs/dataframe_model_custom.md`
- [x] [CI] _(2026-03-04)_ Ajouter docstring check, dbt compile, pip-audit et gitleaks
  à la CI GitHub Actions et aux hooks prek
- [x] [dbt] _(2026-03-04)_ Améliorer configuration dbt et déplacer les index GiST
  silver dans les scripts Postgres
- [x] [Pipeline] _(2026-03-04)_ Corriger l'OOM du diff incrémental silver
  (scan lazy + hash au lieu de full join en mémoire)
- [x] [dbt] _(2026-03-04)_ Valider architecture, scripts et exécution (Airflow et
  local)
- [x] [Infra] _(2026-03-04)_ Migrer Postgres vers PostGIS 17-3.5 pour les jointures
  spatiales gold
- [x] [Pipeline] _(2026-03-03)_ Ajouter le mode incrémental pour l'ingestion
  landing → silver
- [x] [dbt] _(2026-03-02)_ Évaluer dbt/SQLMesh pour la couche gold (agrégations
  cross-datasets, vues matérialisées)
- [x] [dbt] _(2026-03-02)_ Implémenter dbt pour la couche gold
- [x] [Postgres] _(2026-03-02)_ Implémenter le pré-filtrage Polars (delta incrémental)
  pour les requêtes UPSERT
- [x] [Données] _(2026-03-01)_ Intégrer les données météorologiques (climatologie
  Météo France)
- [x] [Docs] _(2026-03-01)_ Créer un script pour reformatter les docstrings
  automatiquement (`scripts/format_docstrings.py`)
- [x] [Postgres] _(2026-03-01)_ Ajouter `updated_at` (`DEFAULT NULL`) sur les tables
  incrémentales
- [x] [Airflow] _(2026-02-28)_ Séparer DAG ingestion et DAG load_pg pour
  re-déclenchement indépendant
- [x] [Postgres] _(2026-02-28)_ Résoudre l'erreur du load_pg_odre_installations (clé
  primaire synthétique)
- [x] [Tests] _(2026-02-28)_ Corriger la création d'exceptions custom directement
  depuis ValueError
- [x] [dbt] _(2026-02-24)_ Étudier le passage à dbt Core pour le chargement
  silver → Postgres
  (écarté : [compte rendu](docs/choix_technique_chargement_postgres.md))
- [x] [Postgres] _(2026-02-24)_ Implémenter le chargement bulk (`COPY`) au lieu du
  row-by-row
