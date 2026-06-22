# Data engineering : ETL of electricity and meteo opendata

## Project overview

Data engineering project that collects, processes, and analyzes French electricity
production data correlated with meteorological data. The goal is to create a unified
data platform correlating weather patterns with electricity production/consumption
across France.

Implements a medallion architecture pipeline :

- **Landing** → **Bronze** → **Silver** → **Gold** layers
- Tech stack: Python 3.13+ (pydantic, httpx, polars, structlog, ...), DuckDB, Docker,
  Postgres, Airflow
- Airflow and Postgres are ran through Docker Compose
- Python's dependency management: `uv` (not pip/poetry)

## Important Notes

- **Airflow**: Local installation is for IDE support only. Actual code runs in Docker.
- **Coverage**: Aim for high coverage but focus on critical paths first.
- **prek**: Runs ruff, ty, and pytest automatically on commit (see `prek.toml`).
- **Python version**: Requires Python 3.13+ (uses modern type syntax).
  TODO: bump to 3.14+ once dbt-core relaxes `mashumaro<3.15` — see pyproject.toml.

## Resources

- See `pyproject.toml for tool configurations (ruff, ty, pytest, coverage, marimo).
- See `.env.example` for all configurable environment variables.

## Quick start commands

### Docker & Airflow

```bash
docker compose down                           # stop services
docker compose up --detach                    # start services
docker compose logs airflow_service --follow  # follow Airflow logs
```

### Python environment setup

```bash
uv sync --upgrade          # Sync and upgrade dependencies
uv run pipeline <dataset>          # run a pipeline (e.g. odre_installations)
uv run pipeline-meteo-clim         # run the Météo France climatologie pipeline
```

### Testing

```bash
uv run pytest # run all tests
```

### Code quality (run before committing)

```bash
uv run ruff check --fix        # Lint and auto-fix issues
uv run ruff format             # Format code (100 char line length)
uv run ty check                # Type checking (must pass)
uv run prek run --all-files    # Run all quality checks + tests
```

### prek hooks

prek hooks automatically run on every commit. Configure with `prek.toml`:

```bash
uv run prek install          # Install hooks (first time only)
uv run prek run --all-files  # Run manually on all files
```

## Git workflow

- **Trunk-based**: a single long-lived branch `main` (no `dev`/`master` split).
- **`main` is protected** — no direct pushes. Every change goes through a
  short-lived branch → PR → **squash-merge**.
- **Squash-merge only** (merge commits and rebase disabled repo-side; head
  branch auto-deleted) → `main` stays linear.
- **CI gate**: `push` on `main` + `pull_request` + `concurrency`. A PR merges
  only when `Code quality & tests` and `Secret scanning` are green and the
  branch is up to date with `main`.
- **Never push to `main` directly** (admin bypass exists for emergencies only).
  Day-to-day:
  ```bash
  git switch -c <type>/<name>            # feat/ fix/ chore/ docs/
  git push -u origin <branch>
  gh pr create --fill                    # PR targets main
  gh pr merge --squash --delete-branch   # once CI is green
  ```

## Language Conventions

- **Code** : English (variable names, function names, class names)
- **Code documentation** : English (docstrings, inline comments)
- **Commits** : French (messages de commit)
- **External documentation** : French (README, docs/*.md)

## Code Style Guidelines

### Imports

- **Always use absolute imports**:
  `from data_eng_etl_electricity_meteo.core.logger import logger`
- **Never use relative imports**: ❌ `from ..core import logger`
- **Import order**: Standard library → Third-party → Local (ruff handles this)

### Type Hints

- **Mandatory**: All function parameters and return values must be typed
- **Use modern syntax**: `list[str]` not `List[str]`, `dict[str, int]` not
  `Dict[str, int]`
- **Pathlib**: Use `Path` for file paths, not `str`
- **Generics**: Use `TypeVar` and `ParamSpec` for generic functions
- **Callbacks**: `Callable` alias for single-function callbacks with simple positional
  signatures (e.g. `MetadataFetcher`, `BronzeTransformFunc`, `BatchProgressFactory`).
  `Protocol` only for multi-method interfaces (e.g. `DownloadProgressReporter`) or
  signatures needing `**kwargs` / positional-only `/` (e.g. `_LogMethod`).

### Function parameters

**Ordering: Subject → Tool → Config** (applies to both positional and keyword-only
slots):

| Position    | Role                                 | Examples                         |
|-------------|--------------------------------------|----------------------------------|
| Positional  | **Subject** — the data being acted on | `dataset`, `df`, `url`, `path`   |
| After `*`   | **Tool** — resource / connection      | `conn`, `cur`, `client`          |
| After `*`   | **Config** — options, flags, timeouts | `timeout`, `progress`, `validate`|

**When to use `*`:**

Use `*` when **argument inversion is a real risk**: 2+ parameters of the same type,
tool/connection parameter (`conn`, `cur`, `client`), or function called from many call
sites. Do not use `*` mechanically — it adds noise when arguments are already
unambiguous by type or naming.

```python
# Distinct types, few callers → no *
def validate_source_columns(
    df: pl.DataFrame, expected_columns: set[str], dataset_name: str
) -> None: ...

# Same types → *
def _should_skip_on_hash(*, previous_hash: str | None, current_hash: str): ...

# Tool parameter → *
def load_silver_to_postgres(
    dataset: RemoteDatasetConfig,
    *,
    conn: psycopg.Connection[Any],
    diff: IncrementalDiffMetrics | None = None,
) -> LoadPostgresMetrics: ...
```

**Call-site convention:**

First argument positional, remaining arguments named. Pydantic models and dataclasses:
all arguments named.

```python
# First arg positional, rest named
validate_source_columns(df, expected_columns=_ALL_SOURCE_COLUMNS,
                         dataset_name="odre_eco2mix_tr")
load_silver_to_postgres(dataset, conn=conn)

# Dataclasses & Pydantic models: all arguments named
HttpDownloadInfo(path=dest_path, file_hash=hasher.hexdigest, size_mib=size_mib)
RemotePathResolver(dataset_name=dataset.name)
```

### Documentation

- **Docstrings**      : Numpy style, mandatory for all public functions/classes
- **Docstring types** : Never repeat type annotations in docstrings (Parameters,
  Attributes sections) — type hints in the signature are the single source of truth.
  Parameter/attribute names have **no trailing colon** (e.g. `param_name`, not
  `param_name:`)
- **Inline comments** : Explain WHY, not WHAT
- **TODO comments**   : Include context and owner when possible

### Docstring formatting (88 chars max per line)

Docstrings have a **max line length of 88 characters** (indentation included), stricter
than the 100-char code limit. The formatter enforces compaction, one-sentence-per-line,
and group integrity automatically — see ``scripts/format_docstrings.py`` for the full
rule set.

Left untouched: one-liners, bullet/numbered lists, numpy section headers/separators,
code blocks (``>>>`` / ``::``), tree diagrams, indented code examples.

Formatting script: `uv run python scripts/format_docstrings.py --diff src/ tests/`

### Comments & section separators

Line length limit: **88 characters** (indentation included), same as docstrings.

**Module level** — 3-line block, 2 blank lines before and after:

```python
# --------------------------------------------------------------------------------------
# Section Name
# --------------------------------------------------------------------------------------


def some_function():
```

**Inside class / function / `if __name__`** — single line filling to 88 chars, 1 blank
line before and after:

```python
    # -- Section Name ------------------------------------------------------------------

    code_here()

    # -- Next Section ------------------------------------------------------------------

    more_code()
```

Trailing dashes: `88 - indent - len("# -- ") - len(text) - 1`.

**Section preamble exception** — a short explanatory comment that applies to the entire
section may be placed immediately after the separator (no blank line), forming a
separator + preamble block. The blank line then goes between the preamble and the first
code element:

```python
    # -- Paths (derived from _ROOT_DIR) ------------------------------------------------
    # DirectoryPath validates that the directory exists at access time.
    # Use plain Path for directories that may not exist yet.

    @computed_field
```

**Explanatory comments** — plain `# text` (no `-- ` prefix), explain WHY not WHAT.

### Error handling

- **Domain exceptions** : Use custom exceptions from `core.exceptions` module
- **Stage wrapping** : Each pipeline stage wraps low-level exceptions in its own
  `*StageError` (`DownloadStageError`, `BronzeStageError`, …). Never let `httpx`,
  `OSError`, `PolarsError` propagate directly from a stage method.
  Programming errors (`TransformNotFoundError`) propagate unwrapped — fast-fail.

#### One `except` per operation

Never wrap multiple operations that raise different exception types in a single
`try/except`. Each `except` must be specific to the operation that raised it.

```python
# BAD — two operations, ambiguous error source
try:
    lf = spec.bronze_transform(landing_path)
    lf.sink_parquet(bronze_path)
except (duckdb.Error, pl.exceptions.PolarsError, OSError) as err:
    raise BronzeStageError("Bronze transform or write failed") from err

# GOOD — one try per operation
try:
    lf = spec.bronze_transform(landing_path)
except (duckdb.Error, pl.exceptions.PolarsError, OSError) as err:
    raise BronzeStageError("Bronze transform failed (reading landing file)") from err

try:
    lf.sink_parquet(bronze_path)
except (pl.exceptions.PolarsError, OSError) as err:
    raise BronzeStageError("Bronze Parquet write failed") from err
```

When a single operation can raise semantically different exception types, use separate
`except` blocks with distinct messages:

```python
# BAD — one message for 6 exception types
except (SchemaValidationError, TransformValidationError,
        SourceSchemaDriftError, duckdb.Error, PolarsError, OSError) as err:
    raise SilverStageError("Silver transform failed") from err

# GOOD — one except per error category
except SourceSchemaDriftError as err:
    raise SilverStageError("Source API schema has changed") from err
except (SchemaValidationError, TransformValidationError) as err:
    raise SilverStageError("Silver output validation failed") from err
except (duckdb.Error, PolarsError, OSError) as err:
    raise SilverStageError("Silver transform or read failed") from err
```

**Acceptable exception**: a single DB transaction with multiple SQL statements sharing
the same rollback logic may use one `try` block with multiple except types.

#### Error message conventions

Messages describe **which operation failed**, not the technical error (the cause carries
that). Include context that helps locate the problem:

- **Table/dataset** when available: `f"Column mismatch … table {qualified_table}"`
- **URL** for download failures: `f"File download failed: {url}"`
- **Direction** for schema mismatches: `"Column 'X' in DataFrame but not in Postgres
  table"` (not `"Missing column: X"`)

#### Cause chaining (`from err` / `from None`)

Always use `from err` or `from None` when raising inside an `except` block — never a
bare `raise SomeError()`, which creates implicit chaining noise
(`"During handling of the above exception, another exception occurred:"`).

- **`from err`** — when the cause carries info **absent** from the wrapper: the error
  message (wrapper says *what* failed, cause says *why*), structured attributes
  (`cause_errors`, `cause_added`), or a discriminating `cause_type`
  (`psycopg.UniqueViolation` vs `SyntaxError`).
- **`from None`** — when the wrapper has **already extracted** the useful info
  via f-string (`reason=f"error parsing YAML: {yaml_error}"`) or dedicated attributes.
  Suppresses noisy library internals from the traceback.

```python
# from err — cause carries the WHY (wrapper only says WHAT failed)
except OSError as err:
    raise BronzeStageError("Bronze Parquet write failed") from err

# from None — wrapper already has str(err) in its attributes
except yaml.YAMLError as yaml_error:
    raise InvalidCatalogError(
        path, reason=f"error parsing YAML: {yaml_error}"
    ) from None
```

#### Catch completeness

Every exception type documented in a function's `Raises` section must be caught by its
caller. When wrapping a function that raises `OSError`, catch `OSError` — do not rely
on the caller to handle it.

#### Docstring `Raises` section

Document **all** exception types that can propagate from a function, including those
raised by callees when they are not caught internally. This is the contract that
callers rely on to write their `except` blocks.

### Terminology & capitalization

- `Parquet` (proper noun, uppercase P — except in filenames like `latest.parquet`)
- `dbt` (always lowercase, even at start of sentence → restructure to avoid)
- `Météo France` (with accent, always)
- `Postgres` (uppercase P)
- `Airflow` (uppercase A)
- `smart-skip` (hyphenated, always)

## Common patterns & best practices

### Base classes

- **Pydantic models** : Always extend `StrictModel` (from `core.pydantic_base`), never
  `BaseModel` directly. `StrictModel` sets `extra="forbid"` to catch typos.
- **Dataclasses** : `@dataclass(frozen=True)` by default (immutable). Add `slots=True`
  for value objects. Only exception: classes with `@cached_property`
  (e.g. `RemoteIngestionPipeline`).
- **Enums** : Always `StrEnum` (not `Enum`), so values serialize as strings.

### Settings (pydantic-settings)

- Singleton `settings` from `core.settings` — instantiated once at import time.
- `@computed_field` + `@cached_property` for derived paths (evaluated lazily on first
  access, not at instantiation).
- `DirectoryPath` validates that the target directory exists at access time.

### Logging (structlog)

- Initialize with `logger = get_logger("category")` at module level.
- Naming convention: `transform` (dataset-specific modules), `transform.shared`
  (shared utilities), `pg_loader`, `pipeline`, `dag.to_silver`,
  `dl.meteo_clim`.
- Always use structured kwargs: `logger.info("message", key=value)`.
- **Never log deterministic values in task execution**: Do not log `dataset_name`,
  medallion paths (e.g. `bronze/{dataset_name}/{version}.parquet`), or any value
  reconstructible from the pipeline context. Airflow already provides task/DAG context.
  Exception: DAG factory loops (parse-time) have no per-task context, so
  `dataset_name=` is allowed at `debug` level to identify which dataset is being
  processed.
- **Do log non-deterministic values**: Log computed values like the current version
  string, file hashes, row counts, or values resolved at runtime (e.g. actual symlink
  target version, detected filename from Content-Disposition).
- **URLs**: Log shortened URLs at `info`/`warning` level (hostname + last path
  segment). Log full URLs at `debug` level **and** in error/exception logs (full URL
  is needed for debugging failures).
- **Field naming convention** — all counts use `{subject}_count` suffix:
  - Generic: `rows_count`, `columns_count`
  - Qualified rows: `rows_added`, `rows_changed`, `rows_unchanged`, `rows_removed`,
    `rows_input`, `rows_output`, `rows_loaded`
  - Other counts: `departments_count`, `renouvelables_count`, `actifs_count`, etc.
- **Log levels**:
  - `debug`     : internal implementation details, developer-only
  - `info`      : pipeline milestones (stage start/end, skip decisions, completion
    metrics)
  - `warning`   : unexpected but handled conditions (fallbacks, data quality issues)
  - `error`     : operation failed but pipeline may continue
  - `critical`  : unrecoverable error, pipeline must stop
  - `exception` : same as `error` but includes traceback (use inside `except` blocks)
- **Diagnostic column prefixes** — columns prefixed `_diag_` or `_warn_` are extracted
  by `extract_diagnostics` and logged automatically:
  - `_diag_` : expected conditions, logged at `info` level ("Diagnostic")
  - `_warn_` : unexpected data quality issues, logged at `warning` level
    ("Data quality issue")
  Use `_diag_` for known, documented conditions (e.g. overseas orphans). Reserve
  `_warn_` for genuinely unexpected situations that operators should investigate.
- **No duplicate logs**: The orchestrator owns `info`-level start/end logs for each
  pipeline stage. Utilities called by the orchestrator must not emit `info` logs for the
  same events — use `debug` only if the log carries information absent from the
  orchestrator log, otherwise remove it entirely. The stage start log must always appear
  **before** any skip or early-return logic, so the trace shows which stage was entered
  even when skipped.

### Path resolution

- **Always use PathResolver**: For all medallion layer path operations.
- **Use `Path` objects**: Never strings for filesystem operations.
- **Compute paths from `_ROOT_DIR`**: All paths relative to the project root.
- **Never hardcode absolute paths**.

### Pipeline manager

- `RemoteIngestionPipeline` orchestrates: download → (extract) → to_bronze → to_silver.
- Pipeline context (`PipelineContext`) accumulates metrics across stages, passed via
  Airflow XCom.
- Serialization: `model_dump(mode="json")`. Deserialization: `Model.model_validate(ctx)`.
- `PipelineRunSnapshot` (subset without ephemeral paths) persisted in Asset metadata for
  smart-skip decisions.

### Transformations

Every dataset module in `transformations/` follows the same layout:

1. Module docstring
2. `logger = get_logger("transform")`
3. Source column constants (`ALL_SOURCE_COLUMNS`, `USED_SOURCE_COLUMNS`)
4. `SilverSchema(DataFrameModel)` — declarative output contract
5. `transform_bronze(landing_path: Path) -> pl.LazyFrame`
6. `transform_silver(lf: pl.LazyFrame) -> pl.LazyFrame`
7. `SPEC = DatasetTransformSpec(...)` — collected by `registry.py`

**LazyFrame vs DataFrame**:
- Bronze returns `LazyFrame` (streaming via `sink_parquet` for large files).
- Silver receives and returns `LazyFrame`. Collection is handled by
  `DatasetTransformSpec.run_silver()`, not by individual transforms.

**Bronze = format conversion, not column renaming**:
- Bronze converts the source format to Parquet (identity for Parquet sources,
  `read_json` for JSON, DuckDB `ST_read` for GeoPackage, etc.).
- Bronze must **preserve source column names** so that `_ALL_SOURCE_COLUMNS`
  matches the real source schema and `validate_source_columns()` detects drift.
- Content conversion is OK (e.g. native geometry → WKB bytes) as long as the
  column name stays the same.

### Spatial data (PostGIS / DuckDB)

Polars has no geometry type. Spatial data flows through the pipeline as **raw WKB
bytes** (`pl.Binary` → Postgres `BYTEA`). The conversion to exploitable PostGIS
`geometry`/`geography` columns is **deferred to dbt staging models**, which are
the first layer with PostGIS access.

- `stg_dim_contours_iris` : `BYTEA` → `geometry` via `ST_GeomFromWKB()`
- `stg_dim_stations_meteo` : `lat/lon` → `geography` via `ST_MakePoint()`

These staging models are **materialized as `table`** (not `view`) because:
1. Avoids re-converting on every query
2. Enables GiST indexes required for spatial operations (KNN `<->`,
   `ST_Contains`, etc.)

### Data validation

- `SilverSchema` extends `DataFrameModel` with `Annotated[type, Column(...)]` fields.
- Constraints: `nullable`, `unique`, `dtype`, `ge`, `le`, `gt`, `lt`, `isin`.
- `validate_source_columns()` detects upstream API schema drift before column selection.
- `validate_not_empty()` guards against empty DataFrames after transform.

### Testing conventions

- Test files: `test_*.py`, test classes: `class Test*:`.
- Parametrize with `@pytest.mark.parametrize(argnames=..., argvalues=...)`.
- No docstrings required on individual test methods (the method name is the doc).

### Performance

- **Never optimize on theoretical estimates.** Always measure with a real benchmark
  before modifying code for performance reasons. Parquet compression, Polars projection
  pushdown, and OS-level caching make theoretical I/O calculations unreliable.
- Benchmark scripts live in `scripts/benchmarks/`.

### Backward compatibility

- When refactoring, DO NOT maintain backward compatibility.

### Separation of concerns

- **Core components** (`core/`): Settings, logging, exceptions, data catalog, base
  classes. No business logic.
- **Low-level utilities** (`utils/`): Pure functions, no business logic.
- **Pipeline logic** (`pipeline/`): Orchestration, business rules.
- **Transformations** (`transformations/`): Dataset-specific bronze/silver transforms.
- **Airflow integration** (`airflow/`): Scheduler, DAG/Asset generation.
