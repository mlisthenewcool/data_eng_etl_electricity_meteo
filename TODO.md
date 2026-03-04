<!-- ===== Conventions de formatage =====
  - Sections       : Maintenant / Ensuite / Plus tard / Terminé
  - Format ouvert  : - [ ] [Domaine] Description (forme impérative)
  - Format terminé : - [x] [Domaine] _(YYYY-MM-DD)_ Description
  - Tri ouvert     : alphabétique par domaine, puis par description
  - Tri terminé    : date décroissante (plus récent en premier)
  ===== -->

## Maintenant

- [ ] [Données] Ajouter les transformations qui permettent de déterminer les stations
  météo à requêter (meilleure station à moins de 20 km par exemple).
- [ ] [Données] Améliorer toutes les transformations : spécifier les données utiles,
  améliorer le calcul des points géographiques, vérifier si DuckDB est vraiment utile ou
  si l'on peut déléguer le calcul des points pour après (par exemple lors du passage
  silver → gold).

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
- [ ] [Pipeline] Capturer les métadonnées de téléchargement custom (par département)
  pour le smart skip de la climatologie
- [ ] [Pipeline] Définir comment intégrer et gérer l'ajout de nouvelles transformations
  silver (impact sur les données existantes)
- [ ] [Pipeline] Étudier la possibilité d'ajouter une barre de progression comme tqdm
  pour le load dans Postgres
- [ ] [Pipeline] Gérer les cas où les données sont là, mais pas les métadonnées : plutôt
  demander à l'utilisateur de choisir au lieu de relancer tout le téléchargement des
  données + tout le pipeline
- [ ] [Pipeline] Implémenter la persistance locale du state (JSON par dataset) pour le
  smart-skip hors Airflow (`cli/run_pipeline.py`)
- [ ] [Pipeline] Vérifier la cohérence des timezones (SQL, Airflow, local)
- [ ] [Pipeline] Vérifier la cohérence des types entre Postgres et Polars
- [ ] [Postgres] Résoudre l'accès concurrent au load dans Postgres
- [ ] [Tests] Ajouter des tests de qualité des données post-load (assertions Polars ou
  SQL : nulls, distribution, cohérence temporelle)
- [ ] [Tests] Ajouter des tests sur les modules et fonctions critiques

## Plus tard

- [ ] [Airflow] Créer un DAG de maintenance pour le nettoyage des fichiers bronze
  obsolètes (`cleanup_old_bronze_versions` existe, mais n'est appelé nulle part)
- [ ] [Airflow] Explorer `@cleanup`, `@teardown`, `task.map` pour simplifier les DAGs
- [ ] [Airflow] Gérer l'incohérence provoquée par le trigger event Asset dans l'UI
- [ ] [Airflow] Intégrer OpenLineage, OpenTelemetry, alertes et SLA
- [ ] [Airflow] Simplifier le nommage des fichiers générés en `frequency.HOURLY`
  (ne pas inclure les minutes et secondes)
- [ ] [Airflow] Uniformiser les logs (scheduler, triggerer, dag-processor, api-server,
  standalone)
- [ ] [Data quality] Évaluer les avantages de Pandera pour la validation de schémas
- [ ] [Données] Implémenter le delta fetch pour `odre_eco2mix_tr` (upsert SQL prêt,
  manque le delta côté source ; séparer les flux def/cons/tr)
- [ ] [Docs] Documenter comment `export AIRFLOW_..._POSTGRES` fonctionne côté Python
- [ ] [Docs] Documenter les variables d'environnement qui configurent le logging
- [ ] [Logging] Ajouter toutes les exceptions custom dans le smoke test visuel
- [ ] [Logging] Rendre les messages d'erreur plus actionnables
- [ ] [Logging] Uniformiser le logging entre exceptions custom et les autres modules
- [ ] [Optimisation] Compression Parquet et suppression de la relecture du DataFrame
  bronze → silver
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
