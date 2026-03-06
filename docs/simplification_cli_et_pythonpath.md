# Simplification des invocations CLI et suppression de PYTHONPATH

> **Statut** : À implémenter — synthèse décisionnelle, mars 2026.

## Problème

Chaque invocation locale de script nécessite du boilerplate :

- `PYTHONPATH=src` — le package n'est pas installé dans le venv (pas de
  `[build-system]` dans `pyproject.toml`)
- `--env-file=.env.local` — overrides locaux (`POSTGRES_HOST=localhost`, etc.)
- Chemins longs vers les scripts (`src/data_eng_etl_electricity_meteo/cli/run_pipeline.py`)

Situation actuelle par contexte :

| Contexte | Invocation |
|---|---|
| CLI (`src/.../cli/`) | `uv run --env-file=.env.local src/.../cli/run_pipeline.py args` |
| Benchmarks (`scripts/`) | `PYTHONPATH=src uv run python scripts/benchmarks/bench_pg.py` |
| dbt | `./scripts/dbt.sh run --select gold` (wrapper shell, cohérent) |
| Tests | `pythonpath = ["src"]` dans `pyproject.toml` (automatique) |
| Docker / Airflow | `PYTHONPATH=/opt/airflow/src` dans le Dockerfile |

## Cause racine

`pyproject.toml` n'a pas de section `[build-system]`. Sans elle, `uv sync` installe
les dépendances mais pas le package lui-même. Python ne sait pas résoudre
`import data_eng_etl_electricity_meteo` sans `PYTHONPATH=src`.

## Solutions évaluées

### 1. `[build-system]` + `[project.scripts]` (packaging standard)

Ajouter dans `pyproject.toml` :

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project.scripts]
pipeline = "data_eng_etl_electricity_meteo.cli.run_pipeline:app"
meteo-clim = "data_eng_etl_electricity_meteo.cli.run_meteo_climatologie:app"
```

- Élimine PYTHONPATH (package installé en editable par `uv sync`)
- Entry points courts (`uv run pipeline ...`)
- Garde `--env-file=.env.local` (uv ne sait pas charger d'env-file automatiquement)

### 2. Wrapper shell (comme `dbt.sh`)

Un script `scripts/run.sh` qui source `.env` + `.env.local` et forward les arguments.

- Cohérent avec `scripts/dbt.sh`
- Élimine `--env-file=.env.local`
- Ne résout pas PYTHONPATH ni les chemins longs

### 3. Hybride : `[build-system]` + wrapper shell (recommandé)

Combiner les deux :

1. `[build-system]` élimine PYTHONPATH de manière standard
2. `[project.scripts]` fournit des entry points courts
3. `scripts/run.sh` charge `.env` + `.env.local` et appelle `uv run "$@"`

Invocation finale :

```bash
./scripts/run.sh pipeline odre_installations
./scripts/run.sh pipeline meteo_france_climatologie --skip-postgres
./scripts/run.sh bench-pg --dataset odre_installations --json results.json
```

### 4. direnv (`.envrc`)

Charge automatiquement les variables d'environnement en entrant dans le répertoire.

- Dépendance externe (direnv doit être installé)
- Implicite — un nouveau dev ne comprend pas pourquoi ça marche
- Ne résout pas PYTHONPATH sans build-system

### 5. `python-dotenv` dans le code

Charger `.env.local` depuis Python au démarrage de chaque entry point.

- PYTHONPATH doit être résolu *avant* l'import, donc python-dotenv ne peut pas le
  résoudre lui-même
- Mélange configuration et code
- Pydantic-settings charge déjà `.env` — risque de confusion sur la priorité

### 6. Makefile / Taskfile

Centralise les commandes avec des targets.

- Syntaxe `ARGS=` peu naturelle pour les commandes avec arguments variés
- Ne résout pas PYTHONPATH sans build-system

## Comparatif

| Critère | 1. build | 2. shell | 3. hybride | 4. direnv | 5. dotenv | 6. Make |
|---|---|---|---|---|---|---|
| Élimine PYTHONPATH | oui | non | **oui** | non | non | non |
| Élimine --env-file | non | oui | **oui** | oui | oui | oui |
| Chemins courts | oui | non | **oui** | non | non | oui |
| Zéro dépendance externe | oui | oui | **oui** | non | non | oui |
| Standard Python | oui | non | **oui** | non | non | non |
| Benchmarks (hors src/) | partiel | oui | **oui** | oui | non | oui |
| Cohérent avec dbt.sh | non | oui | **oui** | non | non | non |

## Décision

**Solution 3 (hybride)** retenue. Étapes d'implémentation :

1. Ajouter `[build-system]` avec hatchling dans `pyproject.toml`
2. Ajouter `[project.scripts]` pour les CLI principaux
3. `uv sync` pour installer le package en mode editable
4. Vérifier que `uv run python -c "import data_eng_etl_electricity_meteo"` fonctionne
   sans PYTHONPATH
5. Créer `scripts/run.sh` (même pattern que `scripts/dbt.sh`)
6. Retirer `PYTHONPATH=src` de `.env.local.example`
7. Retirer `pythonpath = ["src"]` de `[tool.pytest.ini_options]` et
   `[tool.marimo.runtime]` (devenu inutile)
8. Mettre à jour les docstrings Usage dans tous les scripts
9. Mettre à jour `CLAUDE.md` (section Quick start commands)
