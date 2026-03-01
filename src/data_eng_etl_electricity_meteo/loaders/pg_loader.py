"""Postgres silver-layer loader.

Loads silver ``.parquet`` files into the Postgres ``silver`` schema.
This module is **Airflow-agnostic** — it receives an open ``psycopg.Connection`` and has
no knowledge of how it was created (standalone, Hook, test fixture).

Loading strategies (from ``ingestion.mode``):

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
from data_eng_etl_electricity_meteo.pipeline.types import LoadPostgresMetrics

logger = get_logger("loaders.pg_loader")

_COPY_BUFFER_SIZE = 65_536  # 64 KB I/O chunks for psycopg COPY streaming
_COPY_SPOOL_THRESHOLD = 128 * 1024 * 1024  # 128 MB — CSV kept in RAM below, spilled to disk above
# Columns managed server-side (DEFAULT NOW()) — excluded from schema validation.
_SERVER_MANAGED_COLUMNS: set[str] = {"inserted_at", "updated_at"}
_SILVER_SCHEMA = "silver"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_silver_to_postgres(
    dataset_config: RemoteDatasetConfig, conn: psycopg.Connection[Any]
) -> LoadPostgresMetrics:
    """Load a silver ``.parquet`` file into the Postgres silver schema.

    Reads ``ingestion.mode`` from the data catalog to select the strategy:

    - ``snapshot``   : ``TRUNCATE {table}`` then ``COPY`` directly into it.
    - ``incremental``: ``COPY`` into a temp staging table, then execute the
      upsert from ``postgres/upsert/``.

    Manages the transaction entirely: commits on success, rolls back on failure.
    The caller is only responsible for closing the connection.

    Parameters
    ----------
    dataset_config
        Remote dataset configuration from the catalog.
        ``dataset_config.name`` must match a ``postgres/tables/`` DDL file.
    conn
        Open psycopg connection.

    Returns
    -------
    LoadPostgresMetrics

    Raises
    ------
    PostgresLoadError
        On any load failure: missing DDL/upsert file, unreadable parquet, or any
        ``psycopg`` database error.
    """
    mode = dataset_config.ingestion.mode
    silver_path = RemotePathResolver(dataset_config.name).silver_current_path
    pg_table = dataset_config.postgres.table
    qualified_table = f"{_SILVER_SCHEMA}.{pg_table}"

    try:
        ddl_sql = _format_sql_template(
            "tables",
            dataset_config.name,
            schema=_SILVER_SCHEMA,
            table=pg_table,
        )
    except FileNotFoundError as err:
        raise PostgresLoadError() from err

    logger.info(
        "Starting Postgres load",
        dataset=dataset_config.name,
        table=qualified_table,
        strategy=mode,
        silver_path=silver_path,
    )

    try:
        df = pl.read_parquet(silver_path)
    except (pl.exceptions.PolarsError, OSError) as err:
        raise PostgresLoadError() from err

    try:
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
            _validate_columns(cur, df, pg_table)

            if mode == IngestionMode.SNAPSHOT:
                rows = _load_snapshot(cur, df, pg_table)
            else:
                rows = _load_incremental(cur, df, dataset_config.name, pg_table)

        conn.commit()
    except (SchemaValidationError, FileNotFoundError) as err:
        conn.rollback()
        raise PostgresLoadError() from err
    except psycopg.Error as err:
        conn.rollback()
        raise PostgresLoadError() from err

    metrics = LoadPostgresMetrics(
        dataset_name=dataset_config.name,
        table=qualified_table,
        strategy=mode,
        rows_loaded=rows,
    )

    logger.info(
        "Postgres load completed",
        dataset=dataset_config.name,
        table=qualified_table,
        strategy=mode,
        rows_loaded=rows,
    )

    return metrics


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_sql_file(subdir: str, dataset_name: str) -> str:
    """Read ``postgres/{subdir}/{dataset_name}.sql``."""
    path = (settings.postgres_dir_path / subdir / f"{dataset_name}.sql").resolve()

    # Guard against path traversal (e.g. dataset_name="../../etc/passwd")
    if not path.is_relative_to(settings.postgres_dir_path.resolve()):
        raise PostgresLoadError("SQL path escapes postgres directory", path=str(path))

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    return path.read_text(encoding="utf-8").strip()


def _format_sql_template(subdir: str, dataset_name: str, **identifiers: str) -> psql.Composed:
    """Read a SQL template and resolve ``{schema}``, ``{table}``, etc. placeholders.

    Each keyword argument is wrapped in ``psql.Identifier`` for safe quoting.
    """
    # Safe cast: SQL comes from trusted template files on disk, not user input
    template = cast("LiteralString", _read_sql_file(subdir, dataset_name))
    return psql.SQL(template).format(**{k: psql.Identifier(v) for k, v in identifiers.items()})


def _validate_columns(cur: psycopg.Cursor[Any], df: pl.DataFrame, pg_table: str) -> None:
    """Raise ``SchemaValidationError`` if DataFrame and PG table columns diverge."""
    tid = psql.Identifier(_SILVER_SCHEMA, pg_table)
    cur.execute(psql.SQL("SELECT * FROM {table} LIMIT 0").format(table=tid))
    pg_cols = {desc[0] for desc in (cur.description or [])} - _SERVER_MANAGED_COLUMNS
    df_cols = set(df.columns)

    extra = df_cols - pg_cols
    missing = pg_cols - df_cols

    qualified_table = f"{_SILVER_SCHEMA}.{pg_table}"
    if extra or missing:
        raise SchemaValidationError(
            table=qualified_table,
            extra_columns=sorted(extra),
            missing_columns=sorted(missing),
        )


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


def _copy_df(cur: psycopg.Cursor[Any], df: pl.DataFrame, copy_sql: psql.Composed) -> None:
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


def _load_snapshot(cur: psycopg.Cursor[Any], df: pl.DataFrame, pg_table: str) -> int:
    """TRUNCATE then COPY all rows."""
    tid = psql.Identifier(_SILVER_SCHEMA, pg_table)
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    cur.execute(psql.SQL("TRUNCATE {table}").format(table=tid))
    _copy_df(
        cur,
        _prepare_for_copy(df),
        psql.SQL("COPY {table} ({cols}) FROM STDIN (FORMAT CSV)").format(table=tid, cols=cols),
    )

    return len(df)


def _load_incremental(
    cur: psycopg.Cursor[Any],
    df: pl.DataFrame,
    dataset_name: str,
    pg_table: str,
) -> int:
    """COPY to staging, then upsert from ``postgres/upsert/`` SQL file."""
    tid = psql.Identifier(_SILVER_SCHEMA, pg_table)
    staging_name = f"_staging_{dataset_name}"
    staging_id = psql.Identifier(staging_name)
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    upsert_sql = _format_sql_template(
        "upsert",
        dataset_name,
        schema=_SILVER_SCHEMA,
        table=pg_table,
        staging=staging_name,
    )

    # Ensure no leftover staging table from a previous failed run
    cur.execute(psql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=staging_id))
    # LIKE copies column types without constraints; ON COMMIT DROP auto-cleans
    cur.execute(
        psql.SQL("CREATE TEMP TABLE {staging} (LIKE {table}) ON COMMIT DROP").format(
            staging=staging_id,
            table=tid,
        )
    )

    _copy_df(
        cur,
        _prepare_for_copy(df),
        psql.SQL("COPY {staging} ({cols}) FROM STDIN (FORMAT CSV)").format(
            staging=staging_id, cols=cols
        ),
    )

    cur.execute(upsert_sql)

    return cur.rowcount


def run_standalone_postgres_load(dataset_config: RemoteDatasetConfig) -> LoadPostgresMetrics:
    """Open a standalone connection, load silver to Postgres, and close.

    Convenience wrapper for CLI scripts that manages the full connection lifecycle so
    callers only need to ``except PostgresLoadError``.

    Parameters
    ----------
    dataset_config
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
        return load_silver_to_postgres(dataset_config=dataset_config, conn=connection)
    finally:
        connection.close()
