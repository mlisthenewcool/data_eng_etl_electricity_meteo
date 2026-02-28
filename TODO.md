<!-- Sévérité : P0 = bloquant/erreur prod · P1 = important · P2 = souhaitable -->

## P1 – Important

### Tests & fiabilité

- [ ] Ajouter des tests sur les modules/fonctions critiques
- [ ] Gestion des exceptions
    - [ ] Uniformiser le logging entre une exception custom (dans core.exceptions.py) et
      les autres exceptions.
    - [ ] Uniformiser les messages d'erreur et les rendre plus actionnables.
    - [ ] Ajouter toutes les exceptions custom dans le smoke test visuel.

### Données & transformations

- [ ] Ajouter les données météorologiques
    - [ ] Comment automatiser les appels à l'API Météo France ?

### Postgres

- [ ] Uniformiser initialisation des bases, schémas et tables. Tout doit être
  fonctionnel même sans Airflow.

## P2 – Souhaitable

### Pipeline incrémental

- [ ] Ajouter le mode incremental pour l'ingestion landing → silver
- [ ] Implémenter le delta fetch pour `odre_eco2mix_tr` (API ODRE)
    - L'upsert sur un fichier complet n'apporte rien (coût de conflict detection sans
      gain). Le vrai bénéfice de l'incrémental vient du delta fetch côté source.
    - Le fichier SQL d'upsert est prêt (`postgres/upsert/odre_eco2mix_tr.sql`).

### Architecture

- [ ] Comment lancer les jobs dbt avec Airflow ? Avec cosmos ? DAG custom ?
- [ ] Installation de l'extension DuckDB 'spatial' une seule fois en local comme sous
  Airflow ?

### Airflow

- [ ] Gérer l'incohérence provoquée par le trigger event Asset dans l'UI.
- [ ] Uniformiser logs (scheduler, triggerer, dag-processor, api-server, standalone)

### Docker Compose / documentation

- [ ] Configurer et documenter les variables d'environnement qui configurent le logging
- [ ] Expliquer et documenter comment `export AIRFLOW_..._POSTGRES` fonctionne et
  comment le code Python l'utilise

## Résolu

- [x] Résoudre erreur du load_pg_odre_installations (problème de clé primaire).
- [x] Chargement Postgres bulk et non row-by-row.
- [x] Séparer DAG ingestion et DAG load_pg (re-déclenchement indépendant).
- [x] Corriger la création d'exceptions custom directement depuis ValueError sans
  except préalable.
