# Pré-filtrage Polars : delta produit par la couche silver

## Problème

Pour les datasets incrémentaux (`meteo_france_climatologie`), la couche silver produit
un snapshot complet (millions de lignes) et le loader Postgres envoie tout via
`COPY` + staging + upsert. Postgres compare chaque ligne via l'index PK +
`IS DISTINCT FROM` — extrêmement lent quand 95 %+ des lignes sont inchangées.

## Solution

La couche silver calcule le **diff** (current vs backup) en Polars et produit un
`delta.parquet` en plus de `current.parquet`. Le loader Postgres lit `delta.parquet`
au lieu de `current.parquet` pour les incrémentaux.

### Artefacts silver après implémentation

```
silver/meteo_france_climatologie/
├── current.parquet    # snapshot complet (source de vérité)
├── backup.parquet     # snapshot précédent (rotation)
└── delta.parquet      # nouvelles + modifiées uniquement
```

## Calcul du delta (`remote_ingestion.py`)

Dans `to_silver()`, **avant** la rotation silver :

1. Lire le `current.parquet` existant (= previous).
2. Caster `previous` vers le schéma du nouveau DataFrame (`previous.cast(df.schema)`)
   pour aligner les types (Int16 vs Int64, timezone, etc.).
3. **Nouvelles lignes** : anti-join sur la PK (`df.join(previous, on=pk, how="anti")`).
4. **Lignes modifiées** : inner join sur la PK, puis filtre sur toute colonne non-PK
   différente (avec gestion des `null` des deux côtés).
5. `delta = concat([nouvelles, modifiées])`.
6. Rotation silver, écriture de `current.parquet` et `delta.parquet`.

Premier run (pas de `current.parquet` existant) : delta = tout le DataFrame.

## Chargement Postgres (`pg_loader.py`)

| Mode | Source | Stratégie |
|------|--------|-----------|
| Snapshot | `current.parquet` | `TRUNCATE` + `COPY` (inchangé) |
| Incrémental | `delta.parquet` | `COPY` staging + upsert |

- **Delta vide** (0 lignes) → skip, pas de transaction Postgres.
- Après le chargement incrémental : vérification `SELECT COUNT(*)` vs nombre de lignes
  dans `current.parquet`.
- **Mismatch** → fallback full refresh (`TRUNCATE` + `COPY` depuis `current.parquet`).

## Garantie de synchronisation

| Scénario | Comportement |
|----------|-------------|
| Normal | delta appliqué → sync OK |
| Premier run | delta = tout → full upsert → sync OK |
| Load précédent échoué | row count mismatch → full refresh → sync OK |
| Modification manuelle en base | row count mismatch → full refresh → sync OK |
| Aucun changement | delta vide → skip → sync OK |

## Metrics

`IncrementalDiffMetrics` (dans `pipeline/types.py`) :

- `rows_total` : lignes dans le silver complet
- `rows_new` : PK absente du backup → INSERT
- `rows_changed` : PK présente, colonnes différentes → UPDATE
- `rows_unchanged` : identiques → ignorées

Ces métriques sont propagées dans `SilverMetrics.diff` et `LoadPostgresMetrics.diff`.

## Configuration

Le champ `primary_key` dans `catalog.yaml` détermine les colonnes de jointure :

```yaml
# PK simple → string
meteo_france_stations:
  primary_key: "id"

# PK composite → liste
meteo_france_climatologie:
  primary_key: [ "id_station", "date_heure" ]
```

Le champ accepte `str | list[str]`, normalisé en `list[str]` par un `BeforeValidator`
Pydantic dans `RemoteDatasetConfig`.
