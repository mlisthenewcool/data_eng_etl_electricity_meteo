# Stratégie de data quality

## Contexte

Le pipeline suit une architecture medallion : Landing → Bronze → Silver → Gold.

Les fichiers Silver (parquet) sont la **source de vérité** par dataset. La table
Postgres `silver.*` est une copie fidèle chargée via `COPY`. La couche Gold est
produite par dbt dans Postgres.

La validation de qualité des données Silver doit donc se faire **avant l'écriture
du parquet**, pas après — c'est le dernier moment où on peut rejeter des données
invalides.

## Approche actuelle

Trois niveaux de validation Silver, tous en Python custom (zéro dépendance
externe) :

1. **Structurelle** — `prepare_silver()` (renommage snake_case, drop colonnes
   full-null) + `validate_not_empty()` dans le registry wrapper.
2. **Détection de drift source** — `validate_source_columns()` en début de
   `transform_silver()` : vérifie que les colonnes post-`prepare_silver` n'ont
   pas changé. Lève `SourceSchemaDriftError` avec détail des colonnes
   ajoutées/supprimées.
3. **Contrat de sortie** — `DataFrameModel` (metaclass + `Annotated[T, Column()]`)
   par dataset : schéma déclaratif avec types Polars exacts, contraintes de
   valeurs (`nullable`, `unique`, `ge`/`le`, `isin`). Lève
   `SchemaValidationError` avec liste d'erreurs détaillées.

Chaque module de transformation (`transformations/*.py`) définit un `SPEC`
(`DatasetTransformSpec`) regroupant :
- `all_source_columns: frozenset[str]` — colonnes attendues en entrée
- `used_source_columns: frozenset[str]` — colonnes effectivement utilisées
- `silver_schema: type[DataFrameModel]` — contrat de sortie
- `bronze_transform` / `silver_transform` — fonctions de transformation

Voir `docs/dataframe_model_custom.md` pour le design détaillé.

## Décision : DataFrameModel custom, Pandera réévalué plus tard

Le `DataFrameModel` custom a été choisi plutôt que Pandera pour les raisons
suivantes :

- **Types Polars complexes** — `List(String)`, `UInt32`, `Binary`,
  `Datetime("us", "UTC")` : Pandera ne les supporte pas nativement.
- **Zéro dépendance** — ~150 lignes de code, pas de package externe.
- **Syntaxe Pydantic** — `Annotated[T, Column(...)]`, familière et lisible.

### Point de bascule vers Pandera

Pandera sera réévalué si :

- Le nombre de datasets dépasse ~10 et que le `DataFrameModel` custom montre
  ses limites de maintenabilité.
- Des besoins de checks statistiques (distribution, corrélation) apparaissent.
- Le support Pandera des types Polars complexes s'améliore.

## Comparatif des solutions étudiées

### Pandera

- **Type** : validation de schéma déclarative en Python (classes)
- **Support Polars** : natif depuis v0.19
- **Paradigme** : `schema.validate(df)` lève une exception ou retourne le DataFrame
- **Forces** : léger (~5 deps), même philosophie que Pydantic (modèle = contrat),
  checks composables (types, nullabilité, unicité, ranges, regex, custom), messages
  d'erreur structurés par colonne
- **Faiblesses** : pas de dashboard/reporting, data synthesis pas supportée avec Polars
- **Verdict** : **retenu pour la future implémentation**

### Soda Core v4

- **Type** : plateforme de data quality SQL-first
- **Support Polars** : indirect via DuckDB in-memory (enregistre le DataFrame comme
  table DuckDB, exécute des checks SQL)
- **Paradigme** : checks déclaratifs en YAML (SodaCL), exécutés en SQL
- **Forces** : anomaly detection, freshness checks, dashboard (Soda Cloud, payant)
- **Faiblesses** : dépendance lourde (~30+ packages), détour absurde pour valider un
  DataFrame en mémoire (conversion en SQL via DuckDB), orienté monitoring en base
  plutôt que validation in-pipeline, SodaCL = DSL supplémentaire à apprendre
- **Verdict** : **écarté** — mal positionné pour de la validation avant écriture
  parquet. Pertinent uniquement pour du monitoring sur des tables en base, ce que
  dbt tests couvre déjà.

### Pointblank

- **Type** : validation fluide (chaînable) + reporting HTML interactif
- **Support Polars** : natif via Narwhals
- **Paradigme** : `Validate(df).col_vals_*().interrogate()` retourne un objet
  résultat, pas le DataFrame
- **Forces** : très léger (~3 deps), rapports HTML pour stakeholders, API intuitive
- **Faiblesses** : orienté reporting/communication plutôt que "fail fast on bad
  data", l'API retourne un résultat à inspecter plutôt que lever une exception
  (nécessite du code wrapper), jeune (2024), API en évolution
- **Verdict** : **écarté** — l'orientation reporting ne correspond pas au besoin de
  validation stricte avant écriture. Intéressant si on avait besoin de rapports de
  qualité pour des stakeholders non-techniques.

### Great Expectations

- **Type** : plateforme de data quality complète
- **Support Polars** : limité (orienté Pandas/Spark)
- **Paradigme** : expectations déclaratives, data docs, profiling
- **Forces** : très riche fonctionnellement, data docs auto-générées
- **Faiblesses** : très lourd, complexe à configurer, mal intégré Polars, overkill
  pour le use case
- **Verdict** : **écarté** — overkill et mauvais support Polars.

### Python custom

- **Type** : fonctions de validation ad-hoc
- **Support Polars** : natif (c'est du code Polars direct)
- **Paradigme** : impératif, `if condition: raise`
- **Forces** : zéro dépendance, contrôle total, debugging direct
- **Faiblesses** : pas de standard, chaque check est ad-hoc, ne scale pas quand on
  veut valider le contenu par dataset (N checks × M datasets), le contrat n'est pas
  lisible comme une spec
- **Verdict** : **utilisé** — `DataFrameModel` custom avec syntaxe
  `Annotated[T, Column()]`, validation de schéma + valeurs, zéro dépendance.

## Résumé par couche

| Couche | Validation | Outil | Quand |
|--------|-----------|-------|-------|
| Silver (parquet) | Structurelle (vide, snake_case, null cols) | `shared.py` | Actuel |
| Silver (parquet) | Drift source (colonnes ajoutées/supprimées) | `validate_source_columns` | Actuel |
| Silver (parquet) | Contenu (types, ranges, unicité, isin) | `DataFrameModel` | Actuel |
| Silver (Postgres) | Intégrité de la copie | `_validate_columns` | Actuel |
| Gold (Postgres) | Tests métier | dbt tests | Futur (avec dbt) |
