---
name: sweep-deps
description: Full dependency sweep procedure (Python libs, GitHub Actions SHA pins, Docker base images, prek hooks, persistent blockers like require-dbt-version and pip-audit suppressions). Use when updating dependencies or bumping any version.
---

# Dependency updates

A full "update everything to the latest compatible versions" sweep must cover
**all** sources below — not just Python libs. Note in the commit body what was
already at the latest (verified) so the next sweep can skip re-checking.

1. **Python libs** — `uv sync --upgrade`, then
   `uv run python scripts/sync_dep_floors.py` to align the `>=` floors in
   `pyproject.toml` with the resolved `uv.lock`.
2. **GitHub Actions** — SHA-pinned with a trailing `# vX.Y.Z` comment. To bump,
   resolve the tag to a **commit** SHA (dereference annotated tags:
   `gh api repos/<o>/<r>/git/refs/tags/<tag>`; if `.object.type == "tag"`,
   follow with `git/tags/<sha>` to get the commit). Update both the SHA and the
   comment.
3. **Docker base images** — bump to the latest **stable** tag only; ignore
   `rc`/`beta` tags. Check Docker Hub tags, not GitHub releases. Covers both
   `airflow.Dockerfile` (apache/airflow) and `docker-compose.yaml`
   (postgis/postgis). After an Airflow image bump, smoke-test beyond the build:
   `docker compose up --detach`, wait for the container to report healthy, then
   `docker exec airflow_container airflow dags list-import-errors` must return
   "No data found".
4. **prek hooks** — `uv run prek autoupdate --freeze` (keeps `rev` SHA-pinned
   with the `# vX.Y.Z` comment; plain `autoupdate` would unpin it).
5. **Persistent blockers** — re-verify each cycle. The `require-dbt-version` in
   `dbt/dbt_project.yml` must be bumped in lockstep with the `dbt-core` floor
   (not covered by Dependabot). `scripts/run_pip_audit.py` is the single source
   of truth for `--ignore-vuln` (pip-audit has no config file) and is invoked by
   both `ci.yml` and `prek.toml`; every suppression names the blocker that
   justifies it plus the `fixed_in` version that retires it, and the script
   exits 1 before auditing once `uv.lock` resolves that blocker at or past
   `fixed_in`. Blockers therefore self-report, and this section never has to
   restate the advisory ids. Currently open: none — `_SUPPRESSIONS` is empty.
   (Lifted, keep for context: the four sqlparse advisories — dbt-core 1.12.3
   relaxed `sqlparse>=0.5.5,<0.6.0`, resolved to 0.6.0, which carries every
   fix; GHSA-9xwg-3r6f-jcx2 — marimo 0.24.0 dropped the `pymdown-extensions<11`
   cap, resolved to 11.0.1; the ty `<0.0.58` pin — ParamSpec regression on
   airflow-task-sdk's `Task` protocol,
   https://github.com/astral-sh/ty/issues/3957 — fixed in ty 0.0.59; and the
   Python 3.14 block — dbt-core 1.12.0 relaxed `mashumaro<3.18` and ships a
   3.14 classifier, so the project moved to 3.14.)

Verify the sweep with `ruff check` + `ty check` + `pytest` before committing.
