<!-- Classification : Now / Next / Later -->
<!-- Format : - [ ] [domaine] description -->

## Now

- [ ] [dbt] Étudier le passage à dbt Core pour l'insertion des fichiers silver dans
  Postgres

## Next

- [ ] [Docs] Documentation minimale du projet V1 (présentation, données, architecture,
  outils)
- [ ] [Tests] Ajouter des tests sur les modules/fonctions critiques
- [ ] [Données] Intégrer les appels aux API Météo France (phase 2/3
  décrites ici : [integration_meteo_france.md](docs/integration_meteo_france.md))
- [ ] [Pipeline] Ajouter le mode incremental pour l'ingestion landing → silver
- [ ] [Données] Implémenter le delta fetch pour `odre_eco2mix_tr` (upsert SQL prêt,
  manque le delta côté source)
- [ ] [Postgres] Gérer les changements de schémas alors que des données sont en prod ?
- [ ] [dbt] Lancer les jobs dbt avec Airflow (cosmos ? DAG custom ?)
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

## Résolu

- [x] [Postgres] Résoudre erreur du load_pg_odre_installations (clé primaire)
- [x] [Postgres] Chargement bulk et non row-by-row
- [x] [Airflow] Séparer DAG ingestion et DAG load_pg (re-déclenchement indépendant)
- [x] [Tests] Corriger la création d'exceptions custom directement depuis ValueError
- [x] [Données] Ajouter les données météorologiques
- [x] [Postgres] Ajouter `updated_at` (DEFAULT NULL) sur les tables incrémentales
