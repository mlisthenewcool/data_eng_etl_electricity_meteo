## Architecture/refactoring

- [x] Est-ce qu'on ne devrait pas ajouter le chargement des données dans Postgres dans
  le même DAG que celui qui crée le fichier parquet Silver ?
    - Non : permettre re-déclenchement indépendant (par exemple après un changement de
      schéma Postgres)
- [ ] Comment lancer les jobs dbt avec Airflow ? Avec cosmos ? DAG custom ?

## Module

- [ ] Gestion des exceptions
    - [ ] Uniformiser le logging entre une exception custom (dans core.exceptions.py) et
      les autres exceptions.
    - [ ] Uniformiser les messages d'erreur et les rendre plus actionnables.
    - [ ] Ajouter toutes les exceptions custom dans le smoke test visuel.
    - [x] Corriger le problème de création d'exceptions custom directement depuis
      ValueError sans except préalable. Voir si ce n'est pas le cas pour d'autres
      exceptions ailleurs.
- [ ] Ajouter des tests sur les modules/fonctions critiques
- [ ] Ajouter le mode incremental pour l'ingestion landing → silver
- [ ] Implémenter le delta fetch pour `odre_eco2mix_tr` (API ODRE)
    - L'upsert sur un fichier complet n'apporte rien (coût de conflict detection sans
      gain). Le vrai bénéfice de l'incrémental vient du delta fetch côté source.
    - Le fichier SQL d'upsert est prêt (`postgres/upsert/odre_eco2mix_tr.sql`).

## DuckDB

- [ ] Installation de l'extension 'spatial' une seule fois en local comme sous Airflow ?

## Datasets/Transformations

- [ ] Ajouter les données météorologiques
    - [ ] Comment automatiser les appels à l'API Météo France ?

## Airflow

- [ ] Gérer l'incohérence provoquée par le trigger event Asset dans l'UI.
- [ ] DAGs
    - [ ] Résoudre erreur du DAG load_pg_odre_installations (problème de clé primaire).

## Postgres

- [ ] Uniformiser initialisation des bases, schémas et tables. Tout doit être
  fonctionnel même sans Airflow.
- [x] Chargement bulk et non row-by-row.

## Docker Compose

- [ ] Configurer et documenter les variables d'environnement qui configurent le logging
- [ ] Expliquer et documenter comment `export AIRFLOW_..._POSTGRES` fonctionne et
  comment le code Python l'utilise
- [ ] Uniformiser logs (scheduler, triggerer, dag-processor, api-server, standalone)