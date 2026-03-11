# Choix technique : DuckDB spatial pour IGN contours IRIS

> **Date** : 2026-03-11
> **Statut** : Conservé (améliorations appliquées)

## Contexte

DuckDB est utilisé dans le projet pour **un seul dataset** (`ign_contours_iris`) et
**deux opérations** :

1. **Lecture du GeoPackage** (bronze) — `ST_read()` lit le fichier `.gpkg` (format
   SQLite/GDAL) et extrait la géométrie en WKB via `ST_AsWKB()`
2. **Calculs géospatiaux** (silver) — `ST_Centroid()` + `ST_Transform(EPSG:2154 →
   EPSG:4326)` + `ST_X()`/`ST_Y()` pour obtenir les centroids IRIS en WGS84

Le code concerné se trouve dans
`src/.../transformations/datasets/ign_contours_iris.py`.

## Problème initial

DuckDB est une dépendance lourde (~100 MB) utilisée pour un seul dataset.
L'implémentation présentait des irritants (désormais corrigés, voir
« Améliorations appliquées ») :

- **Extension spatiale** : gestion conditionnelle `INSTALL`/`LOAD` entre Airflow
  (pré-installé dans le Docker) et local
- **Workaround locks GDAL** : copie du `.gpkg` dans `/tmp` avant lecture, mélangée
  avec la logique de lecture DuckDB
- **Quirk lat/lon** : `ST_Transform` de DuckDB retourne les coordonnées en ordre
  `(lat, lon)` au lieu du standard `(lon, lat)` — `ST_X()` donne la latitude,
  `ST_Y()` la longitude — insuffisamment documenté dans le code
- **Roundtrip geometry** : la géométrie est décomposée en floats `(lat, lon)` dans
  le silver, puis reconstruite en `geometry` via `ST_MakePoint()` dans le gold dbt

## Solutions évaluées

### 1. PostGIS + `sqlite3` (stdlib)

**Principe** : lire le GPKG avec le module `sqlite3` de Python (le GPKG est du
SQLite), supprimer les centroids du silver, et déplacer les calculs géospatiaux dans
PostGIS via un modèle dbt intermédiaire.

**Avantages** :

- -1 dépendance nette (suppression de DuckDB, rien ajouté)
- PostGIS est la référence pour le spatial (20+ ans, standard OGC)
- Pas de quirk lat/lon (PostGIS suit la convention standard)
- Pas de roundtrip geometry → floats → geometry
- Le mode `?mode=ro` de sqlite3 élimine le problème de lock GDAL sans copie `/tmp`

**Inconvénients** :

- **Contrat silver cassé** : `centroid_lat` et `centroid_lon` disparaissent du
  Parquet silver, créant une désynchronisation entre Parquet local et Postgres.
  C'est un problème structurel : le silver est censé être identique partout
- **Parser GPKG Binary custom** : le format GPKG stocke la géométrie avec un header
  propriétaire (magic `GP`, flags, SRS ID, envelope) avant le WKB standard. ~15
  lignes de parsing, format bien spécifié (OGC 12-128r19), mais code custom à
  maintenir et tester
- **Redistribution de la complexité** : le code DuckDB dans Python (~80 lignes)
  est remplacé par un parser GPKG (~15 lignes) + un modèle dbt intermédiaire +
  des tests dbt pour les bornes de centroids. Le budget total de complexité est
  similaire, voire supérieur
- **Centroids indisponibles sans Postgres** : en mode Parquet-only (CLI sans
  base), les centroids n'existent plus

### 2. GeoPandas / PyOGRIO + Shapely + PyProj

**Principe** : remplacer DuckDB par la stack géospatiale Python standard.

**Avantages** :

- API haut niveau, code lisible
- Pas de quirk lat/lon
- Contrat silver préservé

**Inconvénients** :

- **+3 dépendances** pour en retirer 1 — contraire à l'objectif de simplification
- Introduit **Pandas** comme dépendance transitive dans un projet Polars
  (copie mémoire Pandas → Polars à chaque transformation)
- PyOGRIO/Fiona nécessitent GDAL (même source de problèmes de locks)
- Pas de gain architectural

### 3. `ogr2ogr` (CLI GDAL)

**Principe** : appeler le CLI GDAL via `subprocess`.

**Avantages** :

- Zéro dépendance Python
- Présent dans l'image Docker PostGIS

**Inconvénients** :

- `subprocess` : perte de typage, gestion d'erreurs par parsing de stderr
- Non disponible en local sans installer GDAL séparément
- Rupture avec le pattern `transform_*() → LazyFrame` des autres datasets
- Options CLI fragiles entre versions GDAL

## Comparaison synthétique

| Critère                    | DuckDB (nettoyé)            | PostGIS + sqlite3           | GeoPandas stack  | ogr2ogr       |
|----------------------------|-----------------------------|-----------------------------|------------------|---------------|
| Contrat silver             | Intact                      | **Cassé**                   | Intact           | Intact        |
| Dépendances Python nettes  | 0 (existante)               | -1                          | +3               | 0             |
| Complexité spécifique      | ~80 lignes Python           | ~15 lignes + dbt + tests    | ~30 lignes       | ~40 lignes    |
| Quirks                     | lat/lon, extension, /tmp    | Parser GPKG header          | GDAL locks       | CLI fragile   |
| Maturité outil spatial     | Moyenne (extension récente) | Excellente (PostGIS)        | Bonne            | Excellente    |
| Centroids sans Postgres    | Oui                         | Non                         | Oui              | Oui           |

## Décision

**Conserver DuckDB** et nettoyer les irritants existants.

**Justification** :

1. **Le contrat silver est prioritaire** : la synchronisation Parquet ↔ Postgres est
   un invariant du projet. Le casser pour économiser une dépendance est un mauvais
   trade-off
2. **Complexité équivalente** : PostGIS + sqlite3 ne réduit pas la complexité totale
   — elle la redistribue entre un parser GPKG custom, un modèle dbt intermédiaire et
   des tests dbt, pour un budget final similaire ou supérieur
3. **DuckDB est bien maintenu** : releases fréquentes, backed par MotherDuck,
   écosystème en forte croissance. L'extension spatiale est moins mature que PostGIS
   mais suffisante pour les opérations utilisées (centroid, reprojection)
4. **Une seule dépendance** : DuckDB couvre lecture GPKG ET transforms spatiales.
   Les alternatives qui préservent le contrat silver nécessitent plus de dépendances
   (GeoPandas stack) ou des appels subprocess fragiles (ogr2ogr)

## Améliorations appliquées

1. **Extension spatiale simplifiée** : le conditionnel `is_running_on_airflow` /
   `INSTALL spatial` a été supprimé. DuckDB ≥ 0.9 auto-installe les extensions
   manquantes lors du `LOAD` — seul `LOAD spatial` est conservé. En Docker
   l'extension est pré-installée dans l'image ; en local elle est téléchargée au
   premier lancement puis mise en cache

2. **Workaround `/tmp` isolé** : la copie temporaire du GPKG est encapsulée dans
   le context manager `_gpkg_safe_read_path()` avec documentation du pourquoi
   (locks GDAL sur SQLite) et nettoyage garanti

3. **Quirk lat/lon documenté dans le SQL** : des commentaires inline rendent
   l'inversion visible au point exact où elle se produit :

   ```sql
   -- DuckDB PROJ returns (lat, lon) for EPSG:4326
   -- (non-standard axis order)
   ST_X(centroid_wgs84) AS centroid_lat,  -- X = latitude
   ST_Y(centroid_wgs84) AS centroid_lon   -- Y = longitude
   ```

4. **Évaluer périodiquement** : si DuckDB corrige le quirk lat/lon dans une future
   version, ou si le projet ajoute d'autres datasets géospatiaux, cette décision
   pourra être révisée
