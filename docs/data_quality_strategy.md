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

Validations légères dans `transformations/shared.py` :

- `apply_common_silver()` : renommage snake_case + drop des colonnes full-null +
  `validate_not_empty()`
- `validate_not_empty()` : lève `TransformValidationError` si le DataFrame est vide

Ces checks sont **structurels** (invariants universels). Aucune validation de
**contenu** par dataset (types, ranges, patterns, unicité).

## Décision : Python custom maintenant, Pandera plus tard

Pour les 5 datasets actuels avec uniquement des checks structurels, Python custom
suffit. Pandera sera introduit quand des validations de contenu par dataset seront
nécessaires.

### Point de bascule vers Pandera

Pandera devient plus intéressant que Python custom dès que :

- On veut valider le **contenu des colonnes** (types, ranges, valeurs attendues) —
  en custom ça devient vite une forêt de `if/for` par dataset
- On veut un **contrat lisible par dataset** — un schéma Pandera se lit comme une
  spec, du code custom se lit comme du code
- On ajoute régulièrement des datasets — le pattern Pandera est identique à chaque
  fois, le custom diverge

### Intégration prévue

- Schéma Pandera par dataset (ex: `OdreInstallationsSilver(DataFrameModel)`)
- `schema.validate(df)` appelé dans `to_silver()` avant `df.write_parquet()`
- `pandera.errors.SchemaError` wrappé dans `TransformValidationError`
- `apply_common_silver` reste pour le renommage snake_case et le drop de colonnes
  null (nettoyage structurel avant validation Pandera)

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
- **Verdict** : **utilisé actuellement** pour les checks structurels. Sera remplacé
  par Pandera quand des validations de contenu seront nécessaires.

## Résumé par couche

| Couche | Validation | Outil | Quand |
|--------|-----------|-------|-------|
| Silver (parquet) | Structurelle (vide, null) | Python custom | Actuel |
| Silver (parquet) | Contenu (types, ranges, unicité) | Pandera | Futur |
| Silver (Postgres) | Intégrité de la copie | `_validate_columns` | Actuel |
| Gold (Postgres) | Tests métier | dbt tests | Futur (avec dbt) |
