# Intégration des données météorologiques Météo France

## Contexte métier

Le projet corrèle production électrique (données ODRE) et météo (Météo France).
Les stations météo sont déjà intégrées (`meteo_france_stations` → `dim_stations_meteo`),
mais les **données d'observations/climatologie** — mesures horaires de température,
vent,
rayonnement, précipitations, etc. — étaient absentes.

L'objectif est d'ingérer les mesures météo horaires pour les corréler avec :

- **Production solaire** : rayonnement global, durée d'insolation, nébulosité
- **Production éolienne** : vitesse et direction du vent, rafales
- **Consommation** : température (chauffage/climatisation)
- **Production hydraulique** : précipitations

## Sources de données

### Climatologie (Phase 1 — implémentée)

**Source** : data.gouv.fr (miroir de la BDClim Météo France)

Le téléchargement utilise l'API data.gouv.fr pour découvrir les ressources
disponibles par département et période. Deux formats sont proposés :

- **Parquet (Hydra)** : auto-généré par data.gouv.fr via
  [Hydra](https://github.com/datagouv/hydra), ~20x plus petit que le CSV.gz.
  Disponible pour les fichiers dépassant 200 lignes (~48% des départements).
  L'URL est dans `resource.extras["analysis:parsing:parquet_url"]`.
  Cette couverture est **stable** (seuil fixe, pas un backlog de traitement).
- **CSV.gz** : fichiers pré-packagés par département, disponibles pour tous les
  départements.

**Fallback** : si l'API data.gouv.fr est indisponible, les URLs CSV.gz sont
construites à partir du pattern prédictible :

```
https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/HOR/H_{dept}_latest-{year_start}-{year_end}.csv.gz
```

- 95 fichiers (01..19, 20, 21..95) pour la France métropolitaine
- Corse : département **20** (pas 2A/2B — ces fichiers n'existent pas)
- ~196 colonnes par fichier, données horaires sur 2 ans
- Mises à jour quotidiennes (corrections rétroactives)
- Le descripteur des colonnes :
  `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/HOR/H_descriptif_champs.csv`

### Observations temps réel (Phase 2 — future)

**Source** : API Observation Ciblée Météo France (REST synchrone, clé API requise)

### API Climatologie ciblée (Phase 3 — future)

**Source** : API Climatologie Météo France (asynchrone commande/fichier, clé API
requise)

## Architecture

### Tables

```
dim_stations_meteo          (existante)
    │
    │  id_station
    ▼
fact_meteo_horaire          (nouvelle)
    PK: (id_station, date_heure)
    │
    │  jointure temporelle
    ▼
fact_eco2mix_cons_def       (existante)
    PK: (code_insee_region, date_heure)
```

### Pipeline Phase 1 (backfill data.gouv.fr)

```
[Découverte API]               [Bronze]                   [Silver]
  data.gouv.fr/api/1/...        │                           │
  ↓                              │                           │
[Download 95 fichiers]           │                           │
  Parquet Hydra si dispo         │                           │
  CSV.gz sinon         → merge → landing/merged.parquet      │
                                 │                           │
                                 │  ┌─────────────────────┐  │
                                 │  │ Column pruning       │  │
                                 │  │ (196 → 16 colonnes)  │  │
                                 │  │ Typage Float64/Utf8  │  │
                                 │  └─────────────────────┘  │
                                 │                           │
                                 bronze/{version}.parquet    │
                                 │                           │
                                 │    ┌────────────────────┤  │
                                 │    │ Renommer colonnes  │  │
                                 │    │ Parser dates       │  │
                                 │    │ Narrowing casts    │  │
                                 │    └────────────────────┤  │
                                 │                           │
                                 │    silver/current.parquet
                                 │         ↓
                                 │    [Load PG — UPSERT]
                                 │    fact_meteo_horaire
```

Le download est custom (`utils/meteo_download.py`) car il agrège 95 fichiers
en un seul parquet. Les étapes bronze → silver → Postgres suivent le pipeline
standard.

## Paramètres météorologiques

### 16 colonnes sélectionnées

Les données source sont **déjà en unités finales** — aucune conversion n'est
appliquée. Le descripteur officiel Météo France indique « °C et 1/10 » comme
précision au dixième de degré, et non un stockage en dixièmes de Kelvin.

| Paramètre          | Colonne source | Colonne silver     | Unité   | Impact énergie    |
|--------------------|----------------|--------------------|---------|-------------------|
| Rayonnement global | GLO            | rayonnement_global | J/cm²   | Solaire (PV)      |
| Durée d'insolation | INS            | duree_insolation   | minutes | Solaire           |
| Nébulosité         | N              | nebulosite         | octas   | Solaire (inverse) |
| Vitesse du vent    | FF             | vitesse_vent       | m/s     | Éolien (P ~ v³)   |
| Direction du vent  | DD             | direction_vent     | degrés  | Éolien            |
| Rafale max         | FXI            | rafale_max         | m/s     | Éolien (arrêt)    |
| Température        | T              | temperature        | °C      | Consommation      |
| Température max    | TX             | temperature_max    | °C      | Consommation      |
| Température min    | TN             | temperature_min    | °C      | Consommation      |
| Point de rosée     | TD             | point_de_rosee     | °C      | —                 |
| Humidité           | U              | humidite           | %       | —                 |
| Précipitations     | RR1            | precipitations     | mm      | Hydraulique       |
| Pression station   | PSTAT          | pression_station   | hPa     | —                 |
| Pression mer       | PMER           | pression_mer       | hPa     | —                 |

## Stratégie de mise à jour

Les données climatologiques sont **vivantes** — elles sont corrigées rétroactivement :

- 2 dernières années : mises à jour **quotidiennes**
- 1950 à année-2 : mises à jour **mensuelles**
- Avant 1950 : mises à jour **annuelles**

**Stratégie** : ré-ingestion périodique + **UPSERT** sur `(id_station, date_heure)`.
Si rien n'a changé, la ligne reste identique. C'est idempotent.

## Fichiers

| Fichier                                                   | Rôle                                       |
|-----------------------------------------------------------|--------------------------------------------|
| `src/.../utils/meteo_download.py`                         | Download 95 fichiers → merge → parquet     |
| `src/.../transformations/meteo_france_climatologie.py`    | Bronze + Silver transforms                 |
| `postgres/tables/meteo_france_climatologie.sql`           | DDL `fact_meteo_horaire`                   |
| `postgres/upsert/meteo_france_climatologie.sql`           | SQL UPSERT                                 |
| `tests/transformations/test_meteo_france_climatologie.py` | Tests unitaires                            |
| `data/catalog.yaml`                                       | Entrée `meteo_france_climatologie`         |

## Phases futures

### Phase 2 : API Observation (temps réel)

- Client API avec authentification (clé API dans settings/secrets Docker)
- API Observation Ciblée : requête par `station_id`, REST synchrone
- UPSERT dans `fact_meteo_horaire` (observations remplacées par climatologie)

### Phase 3 : API Climatologie (updates ciblés)

- Remplace le download data.gouv.fr par des requêtes API ciblées par station
- Pattern asynchrone (commande → polling → récupération fichier)
- Ne télécharge que les stations sélectionnées
