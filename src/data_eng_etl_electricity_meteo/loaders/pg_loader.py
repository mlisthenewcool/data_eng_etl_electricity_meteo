"""Postgres silver-layer loader.

Loads silver ``.parquet`` files into the Postgres ``silver`` schema.
This module is **Airflow-agnostic** — it receives an open ``psycopg.Connection`` and has
no knowledge of how it was created (standalone, Hook, test fixture).

Loading modes (from ``ingestion.mode``):

- **snapshot**   : ``TRUNCATE`` + ``COPY`` — full table refresh, idempotent.
- **incremental**: ``COPY`` to a temp staging table + upsert from
  ``postgres/upsert/{dataset_name}.sql``.

SQL files are templates with ``{schema}`` / ``{table}`` placeholders, resolved at
runtime via ``psql.SQL().format()`` with ``psql.Identifier`` quoting:
``postgres/tables/{name}.sql`` (DDL) and ``postgres/upsert/{name}.sql``
(upsert with ``IS DISTINCT FROM`` guards).
"""

import tempfile
from typing import Any, LiteralString, cast

import polars as pl
import psycopg
from psycopg import sql as psql

from data_eng_etl_electricity_meteo.core.data_catalog import (
    IngestionMode,
    RemoteDatasetConfig,
)
from data_eng_etl_electricity_meteo.core.exceptions import (
    PostgresLoadError,
    SchemaValidationError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.types import (
    IncrementalDiffMetrics,
    LoadPostgresMetrics,
)

logger = get_logger("pg_loader")

_COPY_BUFFER_SIZE = 65_536  # 64 KB — psycopg recommended sweet spot
# 128 MB — CSV kept in RAM below, spilled to disk above
_COPY_SPOOL_THRESHOLD = 128 * 1024 * 1024
# Columns managed server-side (DEFAULT NOW()) — excluded from schema validation.
_SERVER_MANAGED_COLUMNS: set[str] = {"inserted_at", "updated_at"}
_SILVER_SCHEMA = "silver"

# Polars base type → compatible Postgres data_type (from information_schema).
# Unknown Polars types are silently skipped (no false positive).
_POLARS_TO_PG_COMPATIBLE: dict[str, set[str]] = {
    "Int8": {"smallint", "integer", "bigint"},
    "Int16": {"smallint", "integer", "bigint"},
    "Int32": {"integer", "bigint"},
    "Int64": {"bigint"},
    "UInt8": {"smallint", "integer", "bigint"},
    "UInt16": {"integer", "bigint"},
    "UInt32": {"bigint"},
    "Float32": {"real", "double precision"},
    "Float64": {"double precision"},
    "Boolean": {"boolean"},
    "String": {"text", "character varying"},
    "Date": {"date"},
    "Datetime": {"timestamp with time zone", "timestamp without time zone"},
    "Binary": {"bytea"},
    "List": {"ARRAY"},
}


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def load_silver_to_postgres(
    dataset: RemoteDatasetConfig,
    *,
    conn: psycopg.Connection[Any],
    diff: IncrementalDiffMetrics | None = None,
) -> LoadPostgresMetrics:
    """Load a silver ``.parquet`` file into the Postgres silver schema.

    Reads ``ingestion.mode`` from the data catalog to select the strategy:

    - ``snapshot``   : ``TRUNCATE {table}`` then ``COPY`` directly into it.
    - ``incremental``: read ``delta.parquet`` (new + changed rows only), ``COPY``
      into a temp staging table, then execute the upsert. After commit, verify row count
      sync with the full silver snapshot; on mismatch, fallback to full refresh.

    Manages the transaction entirely: commits on success, rolls back on failure.
    The caller is only responsible for closing the connection.

    Parameters
    ----------
    dataset
        Remote dataset configuration from the catalog.
        ``dataset.name`` must match a ``postgres/tables/`` DDL file.
    conn
        Open psycopg connection.
    diff
        Incremental diff metrics from the silver stage, or ``None``.
        Forwarded into the returned ``LoadPostgresMetrics`` for observability.

    Returns
    -------
    LoadPostgresMetrics

    Raises
    ------
    PostgresLoadError
        On any load failure: missing DDL/upsert file, unreadable parquet, or any
        ``psycopg`` database error.
    """
    mode = dataset.ingestion.mode
    resolver = RemotePathResolver(dataset.name)
    pg_table = dataset.postgres.table
    qualified_table = f"{_SILVER_SCHEMA}.{pg_table}"

    try:
        ddl_sql = _format_sql_template(
            dataset.name,
            subdir="tables",
            schema=_SILVER_SCHEMA,
            table=pg_table,
        )
    except FileNotFoundError as err:
        raise PostgresLoadError("DDL file not found") from err

    # -- Choose source file: delta for incremental, current for snapshot ---------------

    if mode == IngestionMode.INCREMENTAL:
        source_path = resolver.silver_delta_path
    else:
        source_path = resolver.silver_current_path

    logger.info(
        "Starting Postgres load",
        table=qualified_table,
        mode=mode,
    )

    try:
        df = pl.read_parquet(source_path)
    except (pl.exceptions.PolarsError, OSError) as err:
        raise PostgresLoadError("Failed to read silver Parquet") from err

    # -- Early exit for incremental with empty delta -----------------------------------

    if mode == IngestionMode.INCREMENTAL and len(df) == 0:
        logger.info(
            "Postgres load skipped: no new or changed rows",
            table=qualified_table,
        )
        return LoadPostgresMetrics(
            table=qualified_table,
            mode=mode,
            rows_loaded=0,
            diff=diff,
        )

    # -- DDL, schema validation, and load (single transaction) -------------------------

    try:
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
            _validate_columns(df, cur=cur, pg_table=pg_table)

            if mode == IngestionMode.SNAPSHOT:
                rows = _load_snapshot(df, cur=cur, pg_table=pg_table)
            else:
                rows = _load_incremental(df, cur=cur, dataset_name=dataset.name, pg_table=pg_table)

        conn.commit()
    except (SchemaValidationError, FileNotFoundError) as err:
        conn.rollback()
        raise PostgresLoadError("Schema validation or SQL file error") from err
    except psycopg.Error as err:
        conn.rollback()
        raise PostgresLoadError("Database error during load") from err
    except (pl.exceptions.PolarsError, OSError) as err:
        conn.rollback()
        raise PostgresLoadError("Data serialization error during load") from err

    logger.info(
        "Postgres load completed",
        table=qualified_table,
        mode=mode,
        rows_loaded=rows,
    )

    # -- Post-load sync verification for incremental datasets --------------------------

    if mode == IngestionMode.INCREMENTAL:
        fallback_rows = _verify_and_maybe_full_refresh(
            resolver, conn=conn, ddl_sql=ddl_sql, pg_table=pg_table
        )
        if fallback_rows is not None:
            rows = fallback_rows

    return LoadPostgresMetrics(
        table=qualified_table,
        mode=mode,
        rows_loaded=rows,
        diff=diff,
    )


# --------------------------------------------------------------------------------------
# Internal helpers
# --------------------------------------------------------------------------------------


def _read_sql_file(dataset_name: str, *, subdir: str) -> str:
    """Read ``postgres/{subdir}/{dataset_name}.sql``."""
    path = (settings.postgres_dir_path / subdir / f"{dataset_name}.sql").resolve()

    # Guard against path traversal (e.g. dataset_name="../../etc/passwd")
    if not path.is_relative_to(settings.postgres_dir_path.resolve()):
        raise PostgresLoadError("SQL path escapes postgres directory", path=str(path))

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    return path.read_text(encoding="utf-8").strip()


def _format_sql_template(dataset_name: str, *, subdir: str, **identifiers: str) -> psql.Composed:
    """Read a SQL template and resolve ``{schema}``, ``{table}``, etc. placeholders.

    Each keyword argument is wrapped in ``psql.Identifier`` for safe quoting.
    """
    # Safe cast: SQL comes from trusted template files on disk, not user input
    template = cast("LiteralString", _read_sql_file(dataset_name, subdir=subdir))
    return psql.SQL(template).format(**{k: psql.Identifier(v) for k, v in identifiers.items()})


def _validate_columns(df: pl.DataFrame, *, cur: psycopg.Cursor[Any], pg_table: str) -> None:
    """Raise ``SchemaValidationError`` if DataFrame and PG table columns diverge.

    Checks both column **names** (extra / missing) and **type compatibility**
    (Polars dtype vs Postgres data_type from ``information_schema``).
    """
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (_SILVER_SCHEMA, pg_table),
    )
    pg_columns: dict[str, str] = {row[0]: row[1] for row in cur.fetchall()}
    pg_cols = set(pg_columns.keys()) - _SERVER_MANAGED_COLUMNS
    df_cols = set(df.columns)

    extra = df_cols - pg_cols
    missing = pg_cols - df_cols

    errors: list[str] = []
    for col in sorted(extra):
        errors.append(f"Unexpected column: {col}")
    for col in sorted(missing):
        errors.append(f"Missing column: {col}")
    for col in sorted(df_cols & pg_cols):
        polars_type_name = type(df.schema[col].base_type()).__name__
        pg_type = pg_columns[col]
        compatible = _POLARS_TO_PG_COMPATIBLE.get(polars_type_name)
        if compatible is not None and pg_type not in compatible:
            errors.append(f"{col}: Polars {polars_type_name} vs Postgres {pg_type}")

    if errors:
        raise SchemaValidationError(errors)


def _prepare_for_copy(df: pl.DataFrame) -> pl.DataFrame:
    r"""Pre-process ``pl.Binary`` and ``pl.List`` columns for COPY CSV.

    - Binary → BYTEA hex ``\xdeadbeef``
    - List   → PG array literal ``{"a","b","c"}`` (null-safe, quote-escaped)

    List elements are individually double-quoted and inner ``"`` / ``\`` are escaped so
    that values containing commas, braces, or quotes produce valid Postgres array
    literals.
    """
    exprs: list[pl.Expr] = []

    for col_name in df.columns:
        dtype = df.schema[col_name]

        if dtype == pl.Binary:
            exprs.append(
                pl.concat_str([pl.lit("\\x"), pl.col(col_name).bin.encode("hex")]).alias(col_name)
            )
        elif dtype.base_type() == pl.List:
            # Each element is quoted: escape \ → \\, " → \", then wrap in "…"
            quoted_elements = pl.col(col_name).list.eval(
                pl.concat_str(
                    [
                        pl.lit('"'),
                        pl.element()
                        .cast(pl.String)
                        .str.replace_all("\\", "\\\\", literal=True)
                        .str.replace_all('"', '\\"', literal=True),
                        pl.lit('"'),
                    ]
                )
            )
            exprs.append(
                pl.when(pl.col(col_name).is_null())
                .then(pl.lit(None))
                .otherwise(
                    pl.concat_str([pl.lit("{"), quoted_elements.list.join(","), pl.lit("}")])
                )
                .alias(col_name)
            )

    return df.with_columns(exprs) if exprs else df


def _copy_df(df: pl.DataFrame, *, cur: psycopg.Cursor[Any], copy_sql: psql.Composed) -> None:
    """Serialize ``df`` to CSV and stream it via COPY in 64 KB chunks.

    Uses a ``SpooledTemporaryFile`` to keep small DataFrames in memory and spill large
    ones to disk automatically.
    """
    with tempfile.SpooledTemporaryFile(max_size=_COPY_SPOOL_THRESHOLD) as buf:
        df.write_csv(buf, include_header=False)
        buf.seek(0)

        with cur.copy(copy_sql) as copy:
            while chunk := buf.read(_COPY_BUFFER_SIZE):
                copy.write(chunk)


def _verify_sync(
    pg_table: str,
    *,
    cur: psycopg.Cursor[Any],
    expected_count: int,
) -> int | None:
    """Verify that Postgres row count matches expected silver count.

    Returns
    -------
    int | None
        Actual Postgres row count if there is a mismatch, ``None`` if in sync.
    """
    cur.execute(
        psql.SQL("SELECT COUNT(*) FROM {table}").format(
            table=psql.Identifier(_SILVER_SCHEMA, pg_table)
        )
    )
    row = cur.fetchone()
    assert row is not None  # COUNT(*) always returns a row
    actual: int = row[0]
    if actual != expected_count:
        return actual
    return None


def _verify_and_maybe_full_refresh(
    resolver: RemotePathResolver,
    *,
    conn: psycopg.Connection[Any],
    ddl_sql: psql.Composed,
    pg_table: str,
) -> int | None:
    """Check row count sync after incremental load; full refresh on mismatch.

    Parameters
    ----------
    resolver
        Path resolver for reading the full silver snapshot.
    conn
        Open psycopg connection (already committed after initial load).
    ddl_sql
        Pre-formatted DDL SQL for table creation.
    pg_table
        Unqualified Postgres table name.

    Returns
    -------
    int | None
        Rows loaded during full refresh fallback, or ``None`` if already in sync.
    """
    try:
        silver_full = pl.read_parquet(resolver.silver_current_path)
    except (pl.exceptions.PolarsError, OSError) as err:
        logger.warning("Cannot read silver snapshot for sync check", error=str(err))
        return None

    try:
        with conn.cursor() as cur:
            pg_count = _verify_sync(pg_table, cur=cur, expected_count=len(silver_full))
            if pg_count is None:
                return None

            # Mismatch → full refresh from current.parquet
            logger.warning(
                "Full refresh triggered by row count mismatch",
                table=pg_table,
                pg_rows=pg_count,
                parquet_rows=len(silver_full),
            )
            cur.execute(ddl_sql)
            rows = _load_snapshot(silver_full, cur=cur, pg_table=pg_table)

        conn.commit()
    except psycopg.Error as err:
        conn.rollback()
        raise PostgresLoadError("Full refresh fallback failed") from err
    except (pl.exceptions.PolarsError, OSError) as err:
        conn.rollback()
        raise PostgresLoadError("Data serialization error during full refresh") from err

    logger.info("Full refresh fallback completed", table=pg_table, rows_loaded=rows)
    return rows


def _load_snapshot(df: pl.DataFrame, *, cur: psycopg.Cursor[Any], pg_table: str) -> int:
    """TRUNCATE then COPY all rows."""
    tid = psql.Identifier(_SILVER_SCHEMA, pg_table)
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    cur.execute(psql.SQL("TRUNCATE {table}").format(table=tid))
    _copy_df(
        _prepare_for_copy(df),
        cur=cur,
        copy_sql=psql.SQL("COPY {table} ({cols}) FROM STDIN (FORMAT CSV)").format(
            table=tid, cols=cols
        ),
    )

    return len(df)


def _load_incremental(
    df: pl.DataFrame,
    *,
    cur: psycopg.Cursor[Any],
    dataset_name: str,
    pg_table: str,
) -> int:
    """COPY to staging, then upsert from ``postgres/upsert/`` SQL file."""
    tid = psql.Identifier(_SILVER_SCHEMA, pg_table)
    staging_name = f"_staging_{dataset_name}"
    staging_id = psql.Identifier(staging_name)
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    # -- Prepare SQL templates and identifiers -----------------------------------------

    upsert_sql = _format_sql_template(
        dataset_name,
        subdir="upsert",
        schema=_SILVER_SCHEMA,
        table=pg_table,
        staging=staging_name,
    )

    # -- Create staging table and COPY data --------------------------------------------

    # Guard against connection failures that bypass ON COMMIT DROP cleanup
    cur.execute(psql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=staging_id))

    # Matching column types for COPY, no constraints to block staging inserts
    cur.execute(
        psql.SQL("CREATE TEMP TABLE {staging} (LIKE {table}) ON COMMIT DROP").format(
            staging=staging_id,
            table=tid,
        )
    )

    _copy_df(
        _prepare_for_copy(df),
        cur=cur,
        copy_sql=psql.SQL("COPY {staging} ({cols}) FROM STDIN (FORMAT CSV)").format(
            staging=staging_id, cols=cols
        ),
    )

    # -- Execute upsert ----------------------------------------------------------------

    cur.execute(upsert_sql)

    return cur.rowcount


def run_standalone_postgres_load(dataset: RemoteDatasetConfig) -> LoadPostgresMetrics:
    """Open a standalone connection, load silver to Postgres, and close.

    Convenience wrapper for CLI scripts that manages the full connection lifecycle so
    callers only need to ``except PostgresLoadError``.

    Parameters
    ----------
    dataset
        Remote dataset configuration from the catalog.

    Returns
    -------
    LoadPostgresMetrics

    Raises
    ------
    PostgresLoadError
        On connection failure or any load error.
    """
    from data_eng_etl_electricity_meteo.loaders.pg_connection import (  # noqa: PLC0415
        open_standalone_connection,
    )

    try:
        connection = open_standalone_connection()
    except psycopg.OperationalError as err:
        raise PostgresLoadError("Postgres connection failed") from err

    try:
        return load_silver_to_postgres(dataset=dataset, conn=connection)
    finally:
        connection.close()
