r"""PostgreSQL silver-layer loader.

Loads silver ``.parquet`` files into the PostgreSQL ``silver`` schema.

Loading strategies (read from the data catalog's ``ingestion.mode``):

- **snapshot**   : ``TRUNCATE`` + ``COPY`` — full table refresh, idempotent.
- **incremental**: ``COPY`` to a temp staging table + ``INSERT ON CONFLICT``
  — appends new rows and corrects previously seen ones without data loss.

COPY strategy
-------------
Data is sent to PostgreSQL via ``COPY … FROM STDIN (FORMAT CSV)`` using
polars' Rust CSV writer — no Python row loop. The full DataFrame is serialised
to a ``BytesIO`` buffer by ``_prepare_for_copy`` + ``write_csv``, then streamed
to psycopg in ``_COPY_BUFFER_SIZE``-byte chunks.

Two column types require pre-processing before ``write_csv`` because PostgreSQL
COPY CSV expects a specific text representation that differs from Polars'
default CSV output:

- ``pl.Binary`` → BYTEA hex notation: ``\xdeadbeef`` (vectorized via
  ``bin.encode("hex")`` + ``pl.concat_str``).
- ``pl.List(*)`` → PostgreSQL array literal: ``{elem1,elem2}`` (vectorized via
  ``list.join(",")`` wrapped in curly braces). Assumes element values contain
  no commas or double-quotes — true for the parameter-name arrays in this
  project.

Two loading functions cover the two usage modes:

- **Standalone** (scripts, tests): ``open_standalone_connection()`` opens a
  psycopg (v3) connection from ``settings`` (env vars) and Docker secrets.
  Pass it to ``load_to_silver()``.
- **Airflow**: ``load_to_silver_from_hook()`` wraps an Airflow
  ``PostgresHook``; since ``USE_PSYCOPG3=True`` when psycopg3 + SQLAlchemy
  2.x are installed, ``hook.get_conn()`` returns a psycopg3 connection and
  delegates directly to ``load_to_silver()`` — single code path.

SQL files
---------
Each dataset has a corresponding ``postgres/sql/{dataset_name}.sql`` file:

- Snapshot datasets: DDL only (``CREATE TABLE IF NOT EXISTS``).
- Incremental datasets: DDL + upsert section separated by ``-- BEGIN UPSERT``.
  The upsert section references ``_staging_{dataset_name}``, a temp table
  created and destroyed within the same transaction by this module.
"""

import io
from typing import TYPE_CHECKING, Any, LiteralString, cast

import polars as pl
import psycopg
from psycopg import sql as psql

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_eng_etl_electricity_meteo.core.data_catalog import (
    IngestionMode,
    RemoteDatasetConfig,
)
from data_eng_etl_electricity_meteo.core.exceptions import PostgresLoadError
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.pydantic_base import StrictModel
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver

logger = get_logger("loaders.postgres")

_SILVER_SCHEMA = "silver"
_UPSERT_MARKER = "-- BEGIN UPSERT"
_COPY_BUFFER_SIZE = 65_536  # bytes per write() call — 64 KB I/O chunks to psycopg


# =============================================================================
# Metrics
# =============================================================================


class PostgresLoadMetrics(StrictModel):
    """Metrics produced by a silver → PostgreSQL load operation.

    Attributes
    ----------
    dataset_name:
        Dataset identifier.
    table:
        Schema-qualified table name (e.g. ``silver.odre_installations``).
    strategy:
        Loading strategy used: ``"snapshot"`` or ``"incremental"``.
    rows_loaded:
        Number of rows written (for incremental: rows inserted + updated).
    """

    dataset_name: str
    table: str
    strategy: str
    rows_loaded: int


# =============================================================================
# Public API
# =============================================================================


def load_to_silver(
    dataset_config: RemoteDatasetConfig, conn: psycopg.Connection[Any]
) -> PostgresLoadMetrics:
    """Load a silver ``.parquet`` file into the PostgreSQL silver schema.

    Reads ``ingestion.mode`` from the data catalog to select the strategy:

    - ``snapshot``   : ``TRUNCATE {table}`` then ``COPY`` directly into it.
    - ``incremental``: ``COPY`` into a temp staging table, then execute the
      ``INSERT ON CONFLICT`` upsert from the dataset's ``.sql`` file.

    This function commits the transaction on success. The caller is responsible
    for the connection lifecycle (open / close); on failure, the connection is
    left in an aborted-transaction state and must be rolled back by the caller.

    Parameters
    ----------
    dataset_config:
        Remote dataset configuration from the catalog. ``dataset_config.name``
        must match a catalog key and a ``postgres/sql/`` file.
    conn:
        Open psycopg connection. Use ``open_standalone_connection()`` or
        ``load_to_silver_from_hook()`` to create one.

    Returns
    -------
    PostgresLoadMetrics
        Load result: table, strategy, row count.

    Raises
    ------
    PostgresLoadError
        On any load failure: missing SQL file, unreadable parquet, missing
        upsert marker, or any ``psycopg`` database error.
    """
    mode = dataset_config.ingestion.mode
    silver_path = RemotePathResolver(dataset_config.name).silver_current_path
    table = f"{_SILVER_SCHEMA}.{dataset_config.name}"

    logger.info(
        "Starting PostgreSQL load",
        dataset=dataset_config.name,
        table=table,
        strategy=mode,
        silver_path=silver_path,
    )

    try:
        ddl_sql, upsert_sql = _parse_sql_file(dataset_config.name)
    except FileNotFoundError as err:
        raise PostgresLoadError() from err

    try:
        df = pl.read_parquet(silver_path)
    except (pl.exceptions.PolarsError, OSError) as err:
        raise PostgresLoadError() from err

    # Validate incremental pre-condition before touching the database.
    if mode == IngestionMode.INCREMENTAL and upsert_sql is None:
        raise PostgresLoadError() from ValueError(
            f"Dataset '{dataset_config.name}' is incremental but its SQL file "
            f"has no upsert section (missing '{_UPSERT_MARKER}' marker)."
        )

    try:
        with conn.cursor() as cur:
            cur.execute(cast("LiteralString", ddl_sql))
            _validate_columns(cur, df, table)

            if mode == IngestionMode.SNAPSHOT:
                rows = _load_snapshot(cur, df, table)
            else:
                rows = _load_incremental(cur, df, dataset_config.name, table, upsert_sql)  # type: ignore[arg-type]

        conn.commit()
    except (psycopg.Error, ValueError) as err:
        raise PostgresLoadError() from err

    metrics = PostgresLoadMetrics(
        dataset_name=dataset_config.name,
        table=table,
        strategy=str(mode),
        rows_loaded=rows,
    )

    logger.info(
        "PostgreSQL load completed",
        dataset=dataset_config.name,
        table=table,
        strategy=str(mode),
        rows_loaded=rows,
    )

    return metrics


def open_standalone_connection() -> psycopg.Connection[Any]:
    """Open a psycopg connection using settings resolved at startup.

    Credentials (``settings.postgres_user`` / ``settings.postgres_password``)
    are populated by pydantic-settings from whichever source was available:
    env vars (local dev) or Docker secrets files (Docker / Airflow container).
    The loader code is identical in both environments.

    Returns
    -------
    psycopg.Connection
        Open connection. Caller must close it (or use as a context manager).

    Raises
    ------
    OSError
        If credentials are missing from both env vars and Docker secrets.
    psycopg.OperationalError
        If the connection cannot be established.
    """
    if settings.postgres_user is None:
        raise OSError(
            "PostgreSQL user not configured. "
            "Set POSTGRES_USER env var or provide a 'postgres_root_username' Docker secret."
        )
    if settings.postgres_password is None:
        raise OSError(
            "PostgreSQL password not configured. "
            "Set POSTGRES_PASSWORD env var or provide a 'postgres_root_password' Docker secret."
        )

    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.project_db_name,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def load_to_silver_from_hook(
    dataset_config: RemoteDatasetConfig, hook: "PostgresHook"
) -> PostgresLoadMetrics:
    """Load a silver ``.parquet`` into PostgreSQL via an Airflow ``PostgresHook``.

    ``PostgresHook.get_conn()`` returns a psycopg3 connection when
    ``USE_PSYCOPG3`` is ``True`` — which is guaranteed when psycopg3 and
    SQLAlchemy 2.x are both installed (always the case in this project).
    The connection is passed directly to ``load_to_silver``, keeping a single
    code path for all environments.

    Parameters
    ----------
    dataset_config:
        Remote dataset configuration from the catalog.
    hook:
        Airflow ``PostgresHook`` configured for the project database.

    Returns
    -------
    PostgresLoadMetrics
        Load result: table, strategy, row count.

    Raises
    ------
    PostgresLoadError
        On any load failure (propagated from ``load_to_silver``).
    """
    # USE_PSYCOPG3=True → get_conn() returns a psycopg3 connection wrapped in
    # CompatConnection (Airflow's psycopg2/3 abstraction). Cast to the concrete
    # type so load_to_silver's signature stays free of Airflow types.
    # Do NOT use the connection as a context manager here: load_to_silver()
    # commits the transaction itself, so a second commit via __exit__ is
    # redundant. We close the connection explicitly in the finally block.
    conn = hook.get_conn()
    try:
        return load_to_silver(
            dataset_config=dataset_config, conn=cast("psycopg.Connection[Any]", conn)
        )
    finally:
        conn.close()


# =============================================================================
# Internal helpers
# =============================================================================


def _parse_sql_file(dataset_name: str) -> tuple[str, str | None]:
    """Read the SQL file and split it into (ddl, upsert) sections.

    The upsert section starts at the ``-- BEGIN UPSERT`` marker.
    Snapshot datasets have DDL only; incremental datasets have both.

    Parameters
    ----------
    dataset_name:
        Dataset identifier used to locate ``postgres/sql/{name}.sql``.

    Returns
    -------
    tuple[str, str | None]
        ``(ddl_sql, upsert_sql)`` where ``upsert_sql`` is ``None`` for
        snapshot datasets.

    Raises
    ------
    FileNotFoundError
        If the SQL file does not exist.
    """
    sql_file = settings.root_dir / "postgres" / "sql" / f"{dataset_name}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found for dataset '{dataset_name}': {sql_file}")

    content = sql_file.read_text(encoding="utf-8")

    if _UPSERT_MARKER in content:
        parts = content.split(_UPSERT_MARKER, maxsplit=1)
        return parts[0].strip(), parts[1].strip()

    return content.strip(), None


def _validate_columns(cur: psycopg.Cursor[Any], df: pl.DataFrame, table: str) -> None:
    r"""Compare DataFrame columns against the live PostgreSQL table schema.

    Runs after the DDL (``CREATE TABLE IF NOT EXISTS``) so the table is
    guaranteed to exist. Uses ``SELECT * LIMIT 0`` — a no-data query that
    returns column descriptors from the server without reading any rows.

    Two cases are distinguished:

    - **Extra columns in Parquet** (absent from PG table): raised as
      ``ValueError`` — the COPY would fail anyway, but with a cryptic
      PostgreSQL message. The explicit diff makes the schema drift actionable.
    - **Missing columns in Parquet** (present in PG, absent from Parquet):
      raised as ``ValueError`` — COPY would silently insert ``NULL`` or the
      column ``DEFAULT``, masking a schema drift between the Silver
      transformation and the SQL file.

    Parameters
    ----------
    cur:
        Open cursor (within an active transaction).
    df:
        Silver DataFrame about to be loaded.
    table:
        Schema-qualified target table name (e.g. ``"silver.odre_installations"``).

    Raises
    ------
    ValueError
        If there is any column mismatch between the DataFrame and the
        PostgreSQL table, with the full sorted diff included in the message.
    """
    table_id = psql.Identifier(*table.split(".", 1))
    cur.execute(psql.SQL("SELECT * FROM {table} LIMIT 0").format(table=table_id))
    pg_cols: set[str] = {desc[0] for desc in (cur.description or [])}
    df_cols: set[str] = set(df.columns)

    extra = df_cols - pg_cols  # in Parquet, absent from PG → COPY would fail
    missing = pg_cols - df_cols  # in PG, absent from Parquet → COPY inserts NULL/DEFAULT

    problems = []
    if extra:
        problems.append(f"Parquet has extra columns not in PG: {sorted(extra)}")
    if missing:
        problems.append(f"PG table has columns absent from Parquet: {sorted(missing)}")
    if problems:
        raise ValueError(
            f"Schema mismatch for '{table}': {'. '.join(problems)}. "
            "Update the Silver transformation or the SQL schema."
        )


def _prepare_for_copy(df: pl.DataFrame) -> pl.DataFrame:
    r"""Pre-process columns that PostgreSQL COPY CSV cannot receive verbatim.

    Applies two vectorized transformations (no Python loop):

    - ``pl.Binary``  → BYTEA hex notation ``\xdeadbeef``.
    - ``pl.List(*)`` → PostgreSQL array literal ``{elem1,elem2}``.
      Null lists are preserved as CSV null (empty field → PG ``NULL``).

    All other types are left untouched; ``write_csv`` handles them correctly
    (integers, floats, strings, dates, datetimes, booleans, nulls).

    Parameters
    ----------
    df:
        DataFrame to transform.

    Returns
    -------
    pl.DataFrame
        New DataFrame with pre-processed columns, or the original if no
        Binary / List columns are present.
    """
    exprs: list[pl.Expr] = []

    for col_name in df.columns:
        dtype = df.schema[col_name]

        if dtype == pl.Binary:
            # bin.encode("hex") → "deadbeef"; prepend literal \x for PG BYTEA hex format
            exprs.append(
                pl.concat_str([pl.lit("\\x"), pl.col(col_name).bin.encode("hex")]).alias(col_name)
            )
        elif dtype.base_type() == pl.List:
            # list.join(",") → "a,b,c"; wrap in {} for PG array literal {a,b,c}
            # pl.when preserves null lists as null → empty CSV field → PG NULL
            exprs.append(
                pl.when(pl.col(col_name).is_null())
                .then(pl.lit(None))
                .otherwise(
                    pl.concat_str([pl.lit("{"), pl.col(col_name).list.join(","), pl.lit("}")])
                )
                .alias(col_name)
            )

    return df.with_columns(exprs) if exprs else df


def _copy_df(cur: psycopg.Cursor[Any], df: pl.DataFrame, copy_sql: psql.Composed) -> None:
    """Serialise ``df`` to CSV and stream it to an open COPY context.

    Uses polars' Rust CSV writer (no Python row loop). The full CSV is
    buffered in memory then streamed in ``_COPY_BUFFER_SIZE``-byte chunks.

    Parameters
    ----------
    cur:
        Open cursor (within an active transaction).
    df:
        Pre-processed DataFrame (types already converted for COPY CSV).
    copy_sql:
        ``COPY … FROM STDIN (FORMAT CSV)`` statement as a psycopg ``Composed``.

    Raises
    ------
    psycopg.Error
        On any server-side COPY failure (type mismatch, constraint violation, etc.).
    """
    buf = io.BytesIO()
    df.write_csv(buf, include_header=False)
    buf.seek(0)

    with cur.copy(copy_sql) as copy:
        while chunk := buf.read(_COPY_BUFFER_SIZE):
            copy.write(chunk)


def _load_snapshot(
    cur: psycopg.Cursor[Any],
    df: pl.DataFrame,
    table: str,
) -> int:
    """Full table refresh: TRUNCATE then COPY all rows.

    Parameters
    ----------
    cur:
        Open cursor (within an active transaction).
    df:
        Silver DataFrame to load.
    table:
        Schema-qualified target table name (e.g. ``"silver.odre_installations"``).

    Returns
    -------
    int
        Number of rows loaded.

    Raises
    ------
    psycopg.Error
        On TRUNCATE or COPY failure.
    """
    table_id = psql.Identifier(*table.split(".", 1))
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    cur.execute(psql.SQL("TRUNCATE {table}").format(table=table_id))
    _copy_df(
        cur,
        _prepare_for_copy(df),
        psql.SQL("COPY {table} ({cols}) FROM STDIN (FORMAT CSV)").format(table=table_id, cols=cols),
    )

    return len(df)


def _load_incremental(
    cur: psycopg.Cursor[Any],
    df: pl.DataFrame,
    dataset_name: str,
    table: str,
    upsert_sql: str,
) -> int:
    """Upsert via a temporary staging table: COPY to staging, then INSERT ON CONFLICT.

    The staging table (``_staging_{dataset_name}``) is created without
    constraints (``LIKE {table}`` copies column definitions only), filled via
    COPY, and used as the source for the upsert. It is dropped explicitly
    after the upsert to prevent accumulation in long-lived sessions.

    Parameters
    ----------
    cur:
        Open cursor (within an active transaction).
    df:
        Silver DataFrame to load.
    dataset_name:
        Dataset identifier used to derive the staging table name.
    table:
        Schema-qualified target table name (e.g. ``"silver.odre_eco2mix_tr"``).
    upsert_sql:
        SQL from the ``-- BEGIN UPSERT`` section of the dataset's SQL file.

    Returns
    -------
    int
        Number of rows affected by the INSERT ON CONFLICT (inserted + updated).

    Raises
    ------
    psycopg.Error
        On staging table creation, COPY, upsert, or cleanup failure.
    """
    table_id = psql.Identifier(*table.split(".", 1))
    staging_id = psql.Identifier(f"_staging_{dataset_name}")
    cols = psql.SQL(", ").join(psql.Identifier(col) for col in df.columns)

    # Ensure no leftover staging table from a previous failed run in this session
    cur.execute(psql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=staging_id))
    # LIKE copies column types without constraints — staging accepts duplicates
    cur.execute(
        psql.SQL("CREATE TEMP TABLE {staging} (LIKE {table})").format(
            staging=staging_id,
            table=table_id,
        )
    )

    _copy_df(
        cur,
        _prepare_for_copy(df),
        psql.SQL("COPY {staging} ({cols}) FROM STDIN (FORMAT CSV)").format(
            staging=staging_id, cols=cols
        ),
    )

    cur.execute(cast("LiteralString", upsert_sql))

    # rowcount reflects rows affected by INSERT ON CONFLICT (inserted + updated)
    rows = cur.rowcount if cur.rowcount >= 0 else len(df)

    cur.execute(psql.SQL("DROP TABLE IF EXISTS {staging}").format(staging=staging_id))

    return rows
