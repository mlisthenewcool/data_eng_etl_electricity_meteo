<!-- Classification : Now / Next / Later -->
<!-- Format : - [ ] [domaine] description -->

## Now

- [ ] [dbt] Évaluer dbt/SQLMesh pour la couche gold uniquement (agrégations
  cross-datasets, vues matérialisées)
- [ ] [dbt] Implémenter dbt pour la couche gold
- [ ] [Docs] Documentation minimale du projet V1 (présentation, données, architecture,
  outils) : [exemple projet avec bonne documentation](https://github.com/abeltavares/batch-data-pipeline)
- [ ] [Postgres] Implémenter un pré-filtrage avec Polars des lignes qui ont été
  modifiées pour les requêtes UPSERT

## Next

- [ ] [Tests] Ajouter des tests sur les modules/fonctions critiques
- [ ] [Données] Intégrer les appels aux API Météo France (phase 2/3
  décrites ici : [integration_meteo_france.md](docs/integration_meteo_france.md))
- [ ] [Pipeline] Ajouter le mode incremental pour l'ingestion landing → silver
- [ ] [Données] Implémenter le delta fetch pour `odre_eco2mix_tr` (upsert SQL prêt,
  manque le delta côté source)
- [ ] [Postgres] Gérer les changements de schémas alors que des données sont en prod ?
- [ ] [Postgres] Évolution de schéma automatique (`ALTER TABLE ADD COLUMN`) quand le
  Parquet silver contient de nouvelles colonnes absentes de la table PG
- [ ] [Tests] Tests de qualité des données post-load (assertions Polars ou SQL :
  nulls, distribution, cohérence temporelle)
- [ ] [Archi] Installation unique de l'extension DuckDB 'spatial' (local + Airflow)
- [ ] [Airflow] Gérer l'incohérence provoquée par le trigger event Asset dans l'UI

## Later

- [ ] [Logging] Uniformiser le logging entre exceptions custom et les autres
- [ ] [Logging] Uniformiser les messages d'erreur et les rendre plus actionnables
- [ ] [Logging] Ajouter toutes les exceptions custom dans le smoke test visuel
- [ ] [Postgres] Uniformiser initialisation des bases, schémas et tables (sans Airflow)
- [ ] [Airflow] Uniformiser logs (scheduler, triggerer, dag-processor, api-server,
  standalone)
- [ ] [Docs] Documenter les variables d'environnement qui configurent le logging
- [ ] [Docs] Documenter comment `export AIRFLOW_..._POSTGRES` fonctionne côté Python
- [ ] [Docs] Script ou skill pour reformatter les docstrings proprement
- [ ] [Pipeline] Persistance locale du state (JSON par dataset) pour le smart-skip
  hors Airflow (`cli/run_pipeline.py`)
- [ ] [Pipeline] Capturer les métadonnées de téléchargement custom par département
  pour le smart skip de la climatologie
- [ ] [Pipeline] Options `--skip-bronze`, `--skip-silver`, `--skip-postgres` aux
  entrypoints CLI + rétention configurable des fichiers landing (ex : 1 jour)
- [ ] [Airflow] Créer un DAG de maintenance pour le nettoyage des fichiers bronze
  obsolètes (`cleanup_old_bronze_versions` existe, mais n'est appelé nulle part)

## Résolu

- [x] [Postgres] Résoudre erreur du load_pg_odre_installations (clé primaire)
- [x] [Postgres] Chargement bulk et non row-by-row
- [x] [Airflow] Séparer DAG ingestion et DAG load_pg (re-déclenchement indépendant)
- [x] [Tests] Corriger la création d'exceptions custom directement depuis ValueError
- [x] [Données] Ajouter les données météorologiques
- [x] [Postgres] Ajouter `updated_at` (DEFAULT NULL) sur les tables incrémentales
- [x] [dbt] Étudier le passage à dbt Core pour le chargement silver → Postgres
  (écarté : [compte rendu](docs/choix_technique_chargement_postgres.md))
