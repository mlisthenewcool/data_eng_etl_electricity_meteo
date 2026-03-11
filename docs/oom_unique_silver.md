# OOM `unique()` silver : diagnostic, solutions et plan d'action

## Résumé

Les DAGs `*_to_silver` peuvent échouer par OOM (Out of Memory) dans le container
Airflow. La cause racine est l'appel systématique à `unique()` dans les transformations
silver, qui consomme une mémoire disproportionnée par rapport à la taille du DataFrame
— **jusqu'à 4× la taille du DF en surcoût** — dans un container limité à 7.5 GiB.

Le cas déclencheur est `meteo_france_climatologie` (18.9M lignes, pic mesuré de
16.7 GiB), mais le problème est **structurel** : tout dataset dépassant ~2M lignes
avec `unique()` dans un container contraint est à risque, surtout en exécution
parallèle de DAGs.

---

## 1. Contexte

### Infrastructure

| Composant             | Valeur                                         |
|-----------------------|------------------------------------------------|
| Machine hôte          | 31 GiB RAM, Fedora 43, Linux 6.18              |
| Docker                | Docker Desktop for Linux (VM QEMU)             |
| Mémoire VM Docker     | **7.5 GiB** (défaut Docker Desktop)            |
| Airflow au repos      | ~1.2 GiB (scheduler + webserver + triggerer)    |
| Disponible pour tasks | **~6.3 GiB**                                   |
| Polars                | 1.38.1, thread pool = 16 threads               |
| Parallélisme Airflow  | `PARALLELISM=32`, `MAX_ACTIVE_RUNS_PER_DAG=1`  |

### Profil de tous les datasets (bronze)

| Dataset                      |       Lignes | Cols | Disque  | Mémoire   | Doublons | Taux   |
|------------------------------|-------------:|-----:|--------:|----------:|---------:|-------:|
| **meteo_france_climatologie**| **18 945 351** |   16 |  166 MB | 2 380 MB  |   **0**  | 0.00%  |
| odre_eco2mix_cons_def        |    2 628 864 |   32 |   59 MB |   646 MB  |      312 | 0.01%  |
| odre_eco2mix_tr              |      290 304 |   30 |   10 MB |    63 MB  |        0 | 0.00%  |
| odre_installations           |      125 930 |   51 |    4 MB |    42 MB  |   31 460 | 24.98% |
| ign_contours_iris            |       48 512 |    8 |   55 MB |   124 MB  |        0 | 0.00%  |
| meteo_france_stations        |        2 393 |   10 |    8 MB |   106 MB  |        0 | 0.00%  |

**Seul `odre_installations` a un taux de doublons significatif** (25%), mais c'est un
petit dataset (42 MB). Les plus gros datasets (climatologie, eco2mix_cons_def)
ont 0% ou 0.01% de doublons — le `unique()` y supprime presque rien tout en
consommant des GiB de mémoire.

---

## 2. Investigation

### 2.1 Localisation du problème

Le pipeline silver suit ce chemin dans `DatasetTransformSpec.run_silver()` :

```
scan_parquet (lazy)
  → prepare_silver (lazy : snake_case, drop all-null)
  → validate_source_columns (lazy : schema check)
  → transform_silver (lazy : rename, strptime, cast, unique)
  → collect_narrow(lf)          ← MATÉRIALISATION → OOM
  → extract_diagnostics (eager)
  → select schema columns
  → validate_not_empty
  → SilverSchema.validate
  → return DataFrame
```

Le `collect_narrow(lf)` exécute le plan Polars complet d'un coup. Quand le plan
inclut un `unique()`, c'est une opération **bloquante** qui empêche le moteur
streaming de Polars de traiter les données par morceaux.

### 2.2 Mesures mémoire réelles (climatologie, 18.9M lignes)

Mesurées via `/proc/self/status` (VmRSS et VmPeak) — seul moyen de capturer les
allocations Rust/Arrow, invisibles à `tracemalloc` Python.

#### Impact de chaque opération

| Opération                               | RSS delta  | VmPeak     | DF résultat |
|-----------------------------------------|-----------:|-----------:|------------:|
| `scan + select` (lecture seule)         | +2 859 MB  |  6 677 MB  |    2 380 MB |
| + `strptime + cast` (sans unique)       | +3 046 MB  |  6 963 MB  |    2 019 MB |
| **+ `unique(subset=PK)` sur 16 cols**   |**+10 424 MB**|**16 253 MB**| 2 380 MB |
| `unique()` seul (isolé sur DF chargé)   | **+7 613 MB** | +9 566 MB | —        |
| `is_duplicated().any()` (check seul)    | **+72 MB** | —          | —           |

Le `unique()` à lui seul ajoute **+7.6 GiB** de RSS, pour supprimer **0 lignes**.

#### `collect` vs `sink_parquet`

| Méthode                      | RSS après  | VmPeak     | Viable 7.5 GiB ? |
|------------------------------|----------:|-----------:|:-----------------:|
| `collect()` avec `unique()`  |  9 860 MB | 16 721 MB  | ❌ (×2.2)          |
| `sink_parquet` avec `unique()`| 12 511 MB | 23 413 MB | ❌ (×3.1)          |
| `sink_parquet` sans `unique()`|  1 358 MB |  5 159 MB  | ✅                 |

**`sink_parquet` est pire avec `unique()`** — le moteur streaming traite `unique()`
comme une opération bloquante et alloue encore plus de buffers.

#### Impact du nombre de threads

| Threads | `unique()` seul (RSS) | VmPeak   |
|--------:|--------------------:|---------:|
|       1 |          +6 104 MB  | 13 920 MB|
|      16 |          +7 613 MB  | 16 253 MB|

Le surcoût parallèle est ~1.5 GiB. Le coût mono-thread reste 6.1 GiB — le problème
est structurel, pas lié au parallélisme.

### 2.3 Anatomie du coût de `unique()`

La hash table théorique est **~870 MB**, mais le coût réel est **8.8× plus élevé** :

```
Hash table théorique (18.9M entrées) :    867 MB   ← ce qu'on attend
Coût réel mesuré (unique seul) :        7 613 MB   ← ce qui arrive
```

Décomposition des 6.7 GiB supplémentaires :

| Composant                             |    Coût | Explication                                       |
|---------------------------------------|--------:|---------------------------------------------------|
| Hash table pure                       |  870 MB | 18.9M entrées × ~46 bytes                         |
| **Copie intégrale du DF résultat**    | 2 380 MB| Arrow est immutable — `unique()` crée toujours un nouveau DF, même avec 0 suppressions |
| Buffers internes parallèles           | 1 500 MB| 16 threads × partitions de hash × buffers de tri   |
| **Rétention jemalloc**                | 2 800 MB| Pages freed par Rust conservées par l'allocateur   |
| **Total**                             |**7 600 MB**|                                                |

#### Trois mécanismes, aucun n'est un bug

1. **Immutabilité Arrow** — `unique()` ne modifie pas en place. Il crée un nouveau DF
   complet, même pour 0 suppressions. C'est le contrat fondamental d'Arrow (buffers
   immutables, zero-copy entre opérations).

2. **Parallélisme Polars** — chaque thread construit sa propre partition de hash avec
   ses propres buffers temporaires.

3. **jemalloc ne rend pas la mémoire à l'OS** — après `del df` + `gc.collect()`, le
   RSS reste inchangé. L'allocateur Rust conserve les pages libérées pour réutilisation
   future (évite les syscalls `mmap`/`munmap`). Ce n'est pas un leak, c'est un choix de
   performance.

### 2.4 Risque pour les autres datasets

Le problème n'est pas spécifique à climatologie. Estimation du coût de `unique()` par
dataset (basée sur le ratio mesuré ~3.2× la taille du DF en mémoire) :

| Dataset                   | Mémoire DF | `unique()` estimé | Total estimé | Risque          |
|---------------------------|----------:|-----------------:|-------------:|:---------------:|
| meteo_france_climatologie | 2 380 MB  |       +7 600 MB  |   ~10 000 MB | ❌ OOM (mesuré)  |
| odre_eco2mix_cons_def     |   646 MB  |       ~1 500 MB  |    ~2 200 MB | ⚠️ si parallèle |
| odre_eco2mix_tr           |    63 MB  |         ~150 MB  |      ~220 MB | ✅ OK            |
| odre_installations        |    42 MB  |         ~100 MB  |      ~150 MB | ✅ OK            |

`eco2mix_cons_def` (2.6M lignes, en croissance quotidienne) pourrait déclencher un OOM
si exécuté en parallèle avec climatologie ou si le dataset grandit. L'exécution
concurrente de plusieurs silver tasks (autorisée par `PARALLELISM=32`) multiplie le
risque — et la rétention jemalloc fait que la mémoire d'une task terminée n'est pas
disponible pour la suivante.

### 2.5 Doublons : analyse par dataset

| Dataset                   | Doublons | Source des doublons              | Dedup nécessaire ? |
|---------------------------|:--------:|----------------------------------|:------------------:|
| meteo_france_climatologie |    0     | Impossible (UTC + départements non-chevauchants) | Non — données uniques par construction |
| odre_eco2mix_cons_def     |   312    | Corrections/republications       | Oui (0.01%)        |
| odre_eco2mix_tr           |    0     | —                                | Non                |
| odre_installations        | 31 460   | Entrées agrégées (PKs nulles)    | Oui (25%)          |
| ign_contours_iris         |    0     | —                                | Non (pas de dedup) |
| meteo_france_stations     |    0     | —                                | Non (pas de dedup) |

Pour climatologie, les timestamps sont en UTC (pas d'ambiguïté DST) et les stations
sont partitionnées par département (pas de chevauchement). Le commentaire de
`deduplicate_on_composite_key` cite les transitions DST comme source de doublons,
mais c'est impossible en UTC.

---

## 3. Solutions explorées

### A. Augmenter la mémoire Docker Desktop

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | 1 clic (Docker Desktop → Resources → Memory → 20 GiB)|
| Impact mémoire   | Supprime la contrainte container                      |
| Inconvénients    | Ne résout pas le pic de 16 GiB (gaspillage) ; la VM consomme la RAM hôte en permanence ; ne scale pas ; masque le problème |
| **Verdict**      | **Quick fix pour débloquer, pas une solution pérenne** |

### B. Supprimer le `unique()` de climatologie

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | 1 ligne supprimée dans `meteo_france_climatologie.py` |
| Impact mémoire   | 16.7 GiB → ~7 GiB pic (collect sans unique)          |
| Inconvénients    | Perte du filet de sécurité ; ne protège pas si les données source changent ; spécifique à un dataset |
| **Verdict**      | **Trop fragile seul**                                 |

### C. Dedup conditionnel (`is_duplicated` guard)

Vérifier la présence de doublons via `is_duplicated().any()` (+72 MB) avant de lancer
`unique()` (+7.6 GiB). Seuls les datasets avec doublons réels payent le coût.

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | ~15 lignes dans `spec.py` + refactoring de `deduplicate_on_composite_key` |
| Impact mémoire   | **16.7 GiB → ~3 GiB** pour climatologie (0 doublons) |
| Impact autres    | eco2mix_cons_def (312 dups / 2.6M) : unique() déclenché, ~2.2 GiB — OK dans le container |
|                  | installations (31K dups / 126K) : unique() déclenché, ~150 MB — OK |
| Inconvénients    | Aucun significatif — préserve la sécurité, élimine le gaspillage |
| **Verdict**      | **✅ Solution retenue — meilleur ratio effort/impact** |

### D. `run_silver` streaming (`sink_parquet` + validation lazy)

Écrire le silver via `sink_parquet` au lieu de `collect()` + `write_parquet()`.
Validation par agrégations lazy (1 seule ligne collectée).

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | ~150 lignes, 4 fichiers (`spec.py`, `dataframe_model.py`, `shared.py`, `remote_ingestion.py`) |
| Impact mémoire   | ~3 GiB → **~1.4 GiB** (streaming chunks)             |
| Prérequis        | **Le `unique()` doit être absent du LazyFrame** — sinon `sink_parquet` est pire (23 GiB) |
| Inconvénients    | 2 passes sur les données (validation + écriture), refactoring significatif |
| **Verdict**      | **Optimisation future pertinente, non urgente après C** |

### E. Dedup au moment du download (per-département)

Dédupliquer chaque fichier département (~200K lignes) avant le merge des 95 fichiers.

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | ~10 lignes dans `custom_downloads/meteo_climatologie.py` |
| Impact mémoire   | Dedup 200K lignes (instantané) au lieu de 18.9M       |
| Inconvénients    | Change la frontière de responsabilité ; spécifique climatologie |
| **Verdict**      | **Complémentaire mais non nécessaire avec C**         |

### F. Partitionnement des données

Partitionner le Parquet par département, traiter chaque partition indépendamment.

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Effort           | Refactoring lourd (download, path resolver, transforms, validation, merge) |
| Inconvénients    | Complexité disproportionnée                           |
| **Verdict**      | **Over-engineering**                                  |

### G. Téléchargement incrémental (ne pull que les nouvelles données)

| Source                | Filtre date API ? | Verdict                                        |
|-----------------------|:-----------------:|------------------------------------------------|
| data.gouv.fr (Météo)  | ❌                | Fichiers complets, fenêtre glissante 2 ans      |
| OpenDataSoft (ODRE)   | ❌                | Export complet uniquement, pas de filtre date    |
| data.gouv.fr (IGN)    | ❌                | Archive 7z monolithique                          |

Le smart-skip existant (ETag → Last-Modified → hash SHA256) est déjà la meilleure
optimisation côté download.

| **Verdict** | **Non faisable avec les sources actuelles** |
|-------------|---------------------------------------------|

### H. Dedup côté Postgres (UPSERT)

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Inconvénients    | Viole le contrat silver (doublons dans `current.parquet`), fausse le delta incrémental |
| **Verdict**      | **Non retenu**                                        |

### I. `collect(engine="streaming")` ou `sink_parquet` avec `unique()`

| Critère          | Évaluation                                           |
|------------------|------------------------------------------------------|
| Mesuré           | `sink_parquet` + `unique()` = **23.4 GiB** (pire que `collect`) |
| Raison           | `unique()` est bloquant : le streaming doit bufferiser toutes les lignes |
| **Verdict**      | **Ne résout pas le problème tant que `unique()` est dans le plan** |

---

## 4. Solutions retenues

### Phase 1 — Dedup conditionnel (priorité haute)

Guard `is_duplicated().any()` dans `run_silver` : vérifier la présence de doublons
(+72 MB) avant de lancer `unique()` (+7.6 GiB). Le dedup n'est exécuté que si
des doublons existent réellement.

**Impact** : pic 16.7 GiB → ~3 GiB pour climatologie (0 doublons), aucune
régression pour les datasets avec vrais doublons (installations, eco2mix_cons_def).

### Phase 2 — Config Docker Desktop (recommandé)

Augmenter la mémoire de la VM de 7.5 GiB à 12-16 GiB. Marge de sécurité pour
l'exécution parallèle de DAGs et la croissance future des données.

### Phase 3 — `run_silver` streaming (optionnel, futur)

`sink_parquet` + validation lazy pour éliminer toute matérialisation du DataFrame.
Réduit le pic de ~3 GiB à ~1.4 GiB. Pertinent si le dataset grandit
significativement ou si d'autres gros datasets sont ajoutés.

Prérequis : phase 1 (le streaming ne fonctionne pas avec `unique()` dans le plan).

---

## 5. Plan d'implémentation — Phase 1

### Principe

Sortir le `unique()` du LazyFrame pour qu'il ne soit pas inclus dans le plan de
requête Polars. L'appliquer conditionnellement après le collect, guidé par un
diagnostic lazy bon marché.

```
AVANT (unique dans le plan lazy) :
  lf = transform_silver(lf)    ← inclut unique() via deduplicate_on_composite_key
  df = collect_narrow(lf)      ← exécute unique() → 16 GiB pic

APRÈS (dedup conditionnel, post-collect) :
  lf = transform_silver(lf)    ← diagnostic seul, PAS de unique()
  df = collect_narrow(lf)      ← SANS unique → ~3 GiB
  diag = extract duplicate count from df diagnostics
  if diag > 0:                 ← check +72 MB
      df = df.unique(...)      ← seulement si nécessaire
```

### Fichiers impactés

| Fichier | Modification |
|---------|-------------|
| `transformations/shared.py` | `deduplicate_on_composite_key` : remplacer `unique()` par un comptage de doublons lazy (`is_duplicated().sum()`) comme colonne diagnostique |
| `transformations/spec.py` | `DatasetTransformSpec` : ajouter champ `primary_key`. `run_silver` : après collect + extract_diagnostics, appliquer `unique()` conditionnellement si doublons détectés |
| Tests | Ajouter un test pour le guard conditionnel (avec et sans doublons) |

### Détail des modifications

#### `shared.py` — `deduplicate_on_composite_key`

**Actuel** : applique `lf.unique(subset=key_columns, keep="last")` dans le LazyFrame
et ajoute `_diag_duplicate_rows_removed` via `pl.len()` pré/post dedup.

**Nouveau** : ne fait **plus** le `unique()`. Ajoute uniquement une colonne
diagnostique `_diag_duplicate_rows_removed` via
`pl.struct(key_columns).is_duplicated().sum()`. Le `unique()` est délégué à
`run_silver`.

#### `spec.py` — `DatasetTransformSpec` + `run_silver`

**`DatasetTransformSpec`** : nouveau champ `primary_key: list[str]` pour que
`run_silver` puisse appliquer le dedup sans dépendre du transform.

**`run_silver`** : après `collect_narrow(lf)` et `extract_diagnostics(df)` :

1. Lire la valeur diagnostique `_diag_duplicate_rows_removed` (extraite par
   `extract_diagnostics`)
2. Si > 0 : appliquer `df.unique(subset=self.primary_key, keep="last")` en eager
3. Si 0 : continuer sans surcoût

### Résultat attendu

| Dataset                   | Doublons | Guard   | `unique()` ? | Pic estimé |
|---------------------------|:--------:|--------:|:------------:|-----------:|
| meteo_france_climatologie |    0     |  72 MB  | ❌ skip       |  ~3 GiB    |
| odre_eco2mix_cons_def     |   312    |  72 MB  | ✅ apply      | ~2.2 GiB   |
| odre_eco2mix_tr           |    0     |  72 MB  | ❌ skip       |  ~200 MB   |
| odre_installations        | 31 460   |  72 MB  | ✅ apply      |  ~150 MB   |
| ign_contours_iris         |    0     |   —     | —             |  ~250 MB   |
| meteo_france_stations     |    0     |   —     | —             |  ~150 MB   |

---

## 6. Notes complémentaires

### Précédent OOM corrigé (2026-03-04)

Un OOM similaire a été corrigé dans le calcul du diff incrémental silver
(`_compute_incremental_diff_from_files`). La solution a été de passer d'un full join
en mémoire (2 × 18.9M DataFrames) à un scan lazy + hash. Documenté dans
`docs/delta_incremental_silver.md`.

### Rétention mémoire jemalloc

Après `del df` + `gc.collect()`, le RSS reste inchangé. L'allocateur Rust (jemalloc)
conserve les pages libérées pour réutilisation. Cela signifie que si le `unique()` est
déclenché dans un process Airflow (rare), la mémoire n'est jamais retournée à l'OS
pour la durée de vie du process worker.

### Croissance future

Le dataset climatologie couvre une fenêtre glissante de 2 ans (~18.9M lignes,
relativement stable). `eco2mix_cons_def` accumule l'historique quotidien et est en
croissance. Si un dataset dépasse ~5M lignes avec `unique()` dans un container
contraint, le problème réapparaîtra — la phase 3 (streaming) y répondra.

### Téléchargement incrémental — Phase 2/3

L'API REST Météo France (observation/climatologie) supporte des requêtes par
station + plage de dates, mais nécessite une authentification API et un redesign
de l'ingestion. Documenté dans `docs/integration_meteo_france.md`. Non faisable
avec les sources actuelles (fichiers statiques sur data.gouv.fr).
