# Chargement Parquet → Postgres : choix technique

## Contexte

Les fichiers silver (`.parquet`) sont chargés dans Postgres via le module
`loaders/postgres.py`. Deux stratégies coexistent selon le dataset :

- **Snapshot** : `TRUNCATE` + `COPY` — rechargement complet, idempotent.
- **Incremental** : `COPY` vers une table de staging temporaire + `INSERT ON CONFLICT` —
  upsert sans perte de données.

Ce document justifie le choix du mécanisme de transfert retenu.

## Solutions évaluées

### 1. INSERT via `executemany` / `execute_values`

Approche classique : une requête `INSERT INTO … VALUES (…)` par ligne (ou par
batch avec `executemany`). Même avec le batching de psycopg3, chaque ligne
génère un cycle de parsing SQL + allocation côté Postgres.

**Rejeté** : ordre de grandeur 10–50× plus lent que `COPY` pour des volumes
supérieurs à quelques milliers de lignes. Pas adapté au bulk load.

### 2. COPY ligne par ligne : `iter_rows()` + `write_row()`

Premier jet de l'implémentation : le protocole `COPY FROM STDIN` de Postgres
est bien utilisé (un seul flux réseau, pas de parsing SQL par ligne), mais la
sérialisation reste en Python :

```python
# Antipattern : boucle Python sur chaque ligne du DataFrame
with cur.copy("COPY table FROM STDIN") as copy:
    for row in df.iter_rows():  # matérialise tout le DataFrame en tuples Python
        copy.write_row(row)  # un appel psycopg3 par ligne
```

`iter_rows()` est l'antipattern n°1 de Polars : il sort du moteur Rust et
reconstruit chaque ligne en objets Python. L'adaptation de type psycopg3
(mapping `bytes` → BYTEA, `list` → TEXT[]) est un avantage, mais elle est
invoquée ligne par ligne.

**Rejeté comme implémentation finale** : utilise le bon protocole mais détruit
la vectorisation. Conservé comme point de départ identifié dans le codebase.

### 3. ADBC (Arrow Database Connectivity)

API columnar-native basée sur Apache Arrow. Séduisant sur le papier
(sérialisation zero-copy Arrow), mais le driver ADBC Postgres utilise `COPY`
en interne. C'est une abstraction par-dessus le même mécanisme, avec deux
dépendances supplémentaires (`pyarrow`, `adbc-driver-postgresql`) et sans gain
de performance mesurable pour nos volumes.

**Rejeté** : complexité ajoutée sans bénéfice.

### 4. Extension postgres de DuckDB

DuckDB est déjà dans la pile (transformations bronze/silver). Son extension
`postgres` permet d'écrire directement depuis un scan Parquet vers Postgres,
avec un mapping de types automatique (BLOB → BYTEA, LIST → TEXT[]).

**Avantage principal** : pas de preprocessing manuel des types complexes, et le
Parquet est lu en streaming sans charger le DataFrame entier en RAM.

**Raisons d'écarter pour ce projet** :

- **Atomicité brisée en mode incremental.** DuckDB et psycopg3 ouvrent deux
  connexions Postgres indépendantes. Le `COPY` vers la staging et le
  `INSERT ON CONFLICT` vers la table cible se retrouvent dans deux transactions
  séparées — un échec entre les deux laisse la staging commitée sans que
  l'upsert ait eu lieu.
- **Staging permanente requise.** Les tables `TEMP` étant liées à la session,
  elles sont invisibles depuis la deuxième connexion. Il faudrait une staging
  permanente avec une logique de nettoyage dédiée.
- **Extension à installer.** `INSTALL postgres` nécessite un accès réseau
  depuis le conteneur Airflow, ou un bundling dans le Dockerfile.
- **Gain marginal pour nos volumes.** Les datasets les plus lourds comptent
  quelques millions de lignes. Le bottleneck est le réseau Postgres, pas la
  sérialisation Parquet.

### 5. COPY CSV vectorisé via psycopg3 (solution retenue)

`COPY … FROM STDIN (FORMAT CSV)` est le mécanisme de bulk load natif de
Postgres. L'implémentation utilise le writer CSV Rust de Polars
(`df.write_csv()`) pour sérialiser le DataFrame sans boucle Python, puis
streame les bytes en chunks de 64 Ko vers psycopg3.

**Pourquoi ce choix :**

- **Atomicité complète.** `TRUNCATE` + `COPY` (snapshot) et `COPY` staging +
  `INSERT ON CONFLICT` (incremental) s'exécutent dans la même transaction
  psycopg3 — un échec à mi-chemin rollback l'intégralité.
- **Zéro dépendance nouvelle.** psycopg3 et Polars sont déjà présents.
- **Performance suffisante.** La sérialisation vectorisée de Polars (Rust)
  élimine la boucle Python ligne par ligne.
- **Cohérence snapshot/incremental.** Le même mécanisme sert les deux
  stratégies, avec un preprocessing unifié des types complexes.

### 6. dbt-core + dbt-postgres

dbt est l'outil de référence pour les transformations SQL in-database (le « T » de
ELT). L'idée serait de remplacer le loader Python par des modèles dbt qui gèrent le
chargement silver Parquet → Postgres.

**Problème fondamental : dbt ne sait pas lire de fichiers.** dbt opère exclusivement
*dans* la base — il génère du SQL (`CREATE TABLE AS SELECT`, `INSERT INTO ... SELECT`)
et l'exécute contre une base existante. Il n'a aucun mécanisme pour lire un Parquet
depuis le disque.

**Contournements évalués :**

| Approche | Verdict |
|---|---|
| **dbt seeds** | CSV uniquement, < 5 MB recommandé. Inutilisable (100k–10M lignes). |
| **dbt-external-tables** | Ne supporte **pas** Postgres (Snowflake/Redshift/BigQuery). |
| **pg_parquet** (extension PG) | Extension tierce à installer dans l'image Docker, pas une feature dbt. |
| **Pré-charger en staging + dbt** | Garde tout le code Python actuel + ajoute dbt par-dessus — complexité sans gain. |

**Même avec les données déjà dans Postgres, dbt présente d'autres limites :**

- **Performance** : dbt génère `INSERT INTO ... SELECT`, pas de `COPY`. Le bulk load
  via `COPY FROM STDIN` est 3–10× plus rapide pour nos volumes.
- **UPSERT** : la stratégie incrémentale par défaut sur Postgres est `delete+insert`
  (supprime et réinsère toutes les lignes matchées). Notre `ON CONFLICT DO UPDATE` avec
  `IS DISTINCT FROM` est plus précis — il n'écrit que si les valeurs changent, ce qui
  évite des écritures WAL inutiles. Reproduire ce comportement en dbt nécessiterait une
  macro custom.
- **Validation cross-système** : les contrats dbt vérifient les types *dans* la base
  (Postgres vs output du modèle), mais pas la compatibilité Polars↔Postgres. Le mapping
  `_POLARS_TO_PG_COMPATIBLE` du loader actuel n'a pas d'équivalent dbt.
- **Transaction atomique** : dbt gère ses propres transactions par modèle. Le loader
  actuel exécute DDL + validation + load + upsert dans une seule transaction avec
  rollback sur erreur.
- **Dépendances** : +dbt-core, +dbt-postgres, +Cosmos (intégration Airflow) dans
  l'image Docker — trois dépendances supplémentaires.

**Rejeté** : dbt résout un problème différent (transformations SQL in-database). Pour
le chargement de fichiers Parquet dans Postgres, il ajouterait de la complexité sans
supprimer le code existant et sans bénéfice de performance.

**Note** : dbt reste pertinent pour une éventuelle couche **gold** — agrégations
cross-datasets (`fact_eco2mix` × `fact_meteo_horaire` × `dim_stations`), vues
matérialisées pour dashboards. Ce cas d'usage pourra être réévalué quand le besoin se
présentera.

### 7. SQLMesh

Alternative à dbt avec des **Python models** first-class (lecture Parquet via
Polars/Pandas possible). Cependant :

- Fivetran a racheté Tobiko (créateur de SQLMesh) en septembre 2025, puis a fusionné
  avec dbt Labs. L'avenir de SQLMesh comme outil indépendant est **incertain**.
- Reste un outil de transformation : pas de `COPY`, même limitations de performance
  pour le bulk load.
- Réécriture complète du pipeline nécessaire.

**Écarté** : risque stratégique trop élevé pour un bénéfice marginal.

## Synthèse comparative

| Critère | COPY CSV (retenu) | dbt-postgres | SQLMesh |
|---------|-------------------|-------------|---------|
| Lecture Parquet | Polars | Non | Python models |
| Bulk load | `COPY FROM STDIN` — optimal | `INSERT INTO ... SELECT` — 3–10× plus lent | `INSERT` — idem |
| UPSERT `IS DISTINCT FROM` | Natif | Macro custom nécessaire | Custom |
| Validation Polars↔PG | Mapping complet | Non (DB-only) | Partielle |
| Transaction atomique | Oui | Par modèle | Par modèle |
| Dépendances supplémentaires | Aucune | dbt-core, dbt-postgres, Cosmos | sqlmesh |
| Couche gold (futur) | Code SQL à écrire | Excellent | Excellent |

## Pistes d'amélioration du loader actuel

Sans introduire dbt, le loader peut évoluer sur ces axes :

1. **Tests de qualité des données** : assertions Polars ou SQL post-load (vérification
   de nulls, distribution, cohérence temporelle).
2. **Évolution de schéma automatique** : `ALTER TABLE ADD COLUMN` quand le Parquet
   contient de nouvelles colonnes absentes de la table PG (plutôt que de modifier le DDL
   manuellement).
3. **dbt pour la couche gold uniquement** : quand le besoin d'agrégations cross-datasets
   se présentera, évaluer dbt ou SQLMesh pour cette couche spécifique.

## Preprocessing des types non-standard

`write_csv()` ne connaît pas le dialecte COPY de Postgres pour deux types :

| Type Polars  | Format attendu par PG COPY      | Transformation                     |
|--------------|---------------------------------|------------------------------------|
| `pl.Binary`  | `\xdeadbeef` (hex BYTEA)        | `bin.encode("hex")` + préfixe `\x` |
| `pl.List(*)` | `{elem1,elem2}` (array literal) | `list.join(",")` encadré de `{}`   |

Ces deux transformations sont appliquées vectoriellement par Polars avant
l'écriture CSV — aucune boucle Python n'intervient.
