# Choix de visualisation de données

> **Date** : 2026-03-13
> **Statut** : Exploration (pas encore implémenté)
> **Discussion Claude Code** : `visualization_solutions_exploration`

## Contexte

Le projet a besoin d'un outil de visualisation pour trois usages :

1. **BI analytique** : explorer et présenter les corrélations
   production électrique / météo sur la couche gold (dbt → Postgres)
2. **Visualisation géospatiale** : afficher les contours IRIS (polygones
   PostGIS), les stations météo, et les corrélations spatiales
3. **Monitoring pipeline** : suivre la santé des DAGs Airflow (durée des
   runs, taux d'échec, queue lengths)

Stack actuelle : Postgres 17 + PostGIS 3.5, DuckDB, Airflow, dbt,
Docker Compose, notebooks marimo pour l'exploration.

## Outils évalués

Trois outils open-source retenus après élimination de Redash (développement
quasi-mort depuis le rachat par Databricks en 2020).

### Grafana

- **Forces** : monitoring pipeline natif (Airflow expose StatsD/Prometheus),
  très léger (~200-400 Mo), ultra-adopté en France (~600 offres Indeed),
  support Postgres natif
- **Géo** : Geomap panel avec `ST_AsGeoJSON()` — fonctionnel, nécessite du
  formatage SQL côté requête
- **Limites** : pas de DuckDB natif (plugin communautaire), pas d'intégration
  dbt, pas d'espace d'exploration SQL libre (orienté panels/dashboards)
- **Use case principal** : observabilité / monitoring

### Metabase

- **Forces** : time-to-first-dashboard le plus court, query builder visuel
  (accessible aux non-techniques), bonne adoption FR startup/scale-up (~75
  offres Indeed, +35% croissance adoption FR en 2024), intégration dbt
  (metadata/descriptions), déploiement simple (1 conteneur)
- **Géo** : custom region maps avec fichier GeoJSON statique — acceptable pour
  les contours IRIS (changent rarement)
- **Limites** : pas de support DuckDB, rendu géo basique (Leaflet), JVM
  ~1-2 Go RAM, pas adapté au monitoring temps réel
- **Use case principal** : BI self-service

### Apache Superset

- **Forces** : deck.gl natif pour le géospatial (WebGL, rendu GPU), SQL Lab
  (IDE SQL complet), support DuckDB natif (`duckdb-engine`), intégration dbt,
  architecture SQLAlchemy (tout driver compatible fonctionne)
- **Géo** : le plus fluide — deck.gl lit la geometry PostGIS directement, pas
  de pré-traitement, choropleth/scatter/arc/heatmap natifs
- **Limites** : déploiement lourd en production (Flask + Redis + Celery,
  3-4 conteneurs), faible adoption FR (~25 offres Indeed), courbe
  d'apprentissage plus longue
- **Use case principal** : BI analytique pour équipes data SQL-first

## Comparatif synthétique

| Critère                    | Grafana         | Metabase       | Superset        |
|----------------------------|-----------------|----------------|-----------------|
| **Postgres**               | Natif           | Natif          | Natif           |
| **DuckDB**                 | Plugin communaut. | Non          | **Natif**       |
| **PostGIS / géo**          | `ST_AsGeoJSON()` + Geomap | GeoJSON statique | **deck.gl natif** |
| **Monitoring Airflow**     | **Natif** (StatsD/Prometheus) | Non adapté | Non adapté |
| **Intégration dbt**        | Non             | Oui            | Oui             |
| **Adoption FR (Indeed)**   | **~600 offres** | ~75 offres     | ~25 offres      |
| **Empreinte Docker**       | **Légère**      | Moyenne        | Lourde          |
| **Exploration SQL**        | Panels          | Visual + SQL   | **SQL Lab**     |

## Performance géo — rendu navigateur

Les trois délèguent le calcul spatial à PostGIS côté serveur. La différence
est côté rendu navigateur :

- **Grafana** : OpenLayers (rendu CPU)
- **Metabase** : Leaflet (rendu CPU, le plus basique)
- **Superset** : deck.gl (rendu GPU, avantage théorique sur gros volumes)

Pour un affichage par département/région (quelques centaines à milliers de
polygones), les trois suffisent. Pour la France entière (~50 000 polygones
IRIS), deck.gl a un avantage architectural. A benchmarker sur le volume
réel avant de choisir sur ce critère.

## Scénarios d'implémentation

### Option A — Grafana seul

Monitoring pipeline natif + BI acceptable via SQL panels + géo via
`ST_AsGeoJSON()`. Un seul outil, léger. Complété par les notebooks marimo
pour l'exploration ad-hoc.

**Quand choisir** : le monitoring pipeline est prioritaire et les besoins
BI/géo restent modestes.

### Option B — Grafana + Metabase

Séparation des préoccupations : Grafana pour le monitoring Airflow,
Metabase pour le BI analytique sur la couche gold. Le GeoJSON statique
pour les contours IRIS est une contrainte acceptable (contours IGN stables).

**Quand choisir** : on veut un BI accessible (non-techniques) en plus du
monitoring, sans la complexité de déploiement de Superset.

### Option C — Grafana + Superset

Grafana pour le monitoring, Superset pour le BI géospatial avancé.
La combinaison la plus puissante mais la plus lourde à déployer et
maintenir.

**Quand choisir** : le besoin géospatial analytique est central et
l'équipe est à l'aise avec une stack plus complexe.

## Décision

A prendre. Pas de choix arrêté à ce stade.
