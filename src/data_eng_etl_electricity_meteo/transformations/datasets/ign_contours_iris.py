"""Transformations for IGN contours IRIS dataset."""

import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import duckdb
import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec
from data_eng_etl_electricity_meteo.utils.polars import collect_narrow

logger = get_logger("transform")


# --------------------------------------------------------------------------------------
# Silver schema
# --------------------------------------------------------------------------------------


_ALL_SOURCE_COLUMNS: frozenset[str] = frozenset(
    {
        "cleabs",
        "code_insee",
        "code_iris",
        "geometrie",
        "iris",
        "nom_commune",
        "nom_iris",
        "type_iris",
    }
)

# cleabs (internal IGN id) and iris (redundant with code_iris) are not used.
_USED_SOURCE_COLUMNS: frozenset[str] = _ALL_SOURCE_COLUMNS - {"cleabs", "iris"}


class SilverSchema(DataFrameModel):
    """Silver output contract for IGN contours IRIS."""

    code_iris: Annotated[str, Column(nullable=False, unique=True)]
    nom_iris: Annotated[str, Column(nullable=False)]
    code_insee: Annotated[str, Column(nullable=False)]
    nom_commune: Annotated[str, Column(nullable=False)]
    type_iris: Annotated[str, Column(nullable=False, isin=["H", "A", "D", "Z"])]
    geometrie: Annotated[bytes, Column(dtype=pl.Binary(), nullable=False)]
    # Metropolitan France only (overseas territories excluded at source level)
    centroid_lat: Annotated[float, Column(nullable=False, ge=41.0, le=52.0)]
    centroid_lon: Annotated[float, Column(nullable=False, ge=-6.0, le=10.0)]


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


@contextmanager
def _duckdb_spatial_conn() -> Generator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB in-memory connection with the spatial extension loaded.

    ``LOAD`` never downloads a missing extension, hence the explicit ``INSTALL``, a
    no-op once the extension is cached. In Docker the extension is pre-installed in the
    image; locally it is fetched on first use then cached.
    """
    with duckdb.connect(":memory:") as conn:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        conn.execute("SET threads = 1")
        conn.execute("SET memory_limit = '1GB'")
        # DuckDB 2.0 refuses ST_Transform until the axis order is set explicitly;
        # true (x = longitude) is the recommended value and the 2.1 default.
        conn.execute("SET geometry_always_xy = true")
        yield conn


def _select_to_polars(
    query: str, *, conn: duckdb.DuckDBPyConnection, params: list[str] | None = None
) -> pl.DataFrame:
    """Run a read-only DuckDB query and return its result as a Polars DataFrame.

    DuckDB 2.0 removed ``.pl()`` from ``execute()`` results; only relations built by
    ``sql()`` expose it.

    Raises
    ------
    duckdb.Error
        On DuckDB query failure.
    """
    rel = conn.sql(query, params=params)
    assert rel is not None  # guaranteed: sql() returns None only for non-SELECT
    return rel.pl()


@contextmanager
def _gpkg_safe_read_path(gpkg_path: Path) -> Generator[Path]:
    """Copy a GeoPackage to a temp location to avoid GDAL file locks.

    GeoPackage (.gpkg) is SQLite-based.
    GDAL acquires file locks when reading, which causes indefinite hangs in Airflow's
    LocalExecutor if another process holds a lock on the same file.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_gpkg = Path(tmp_dir) / gpkg_path.name
        shutil.copy2(gpkg_path, tmp_gpkg)
        yield tmp_gpkg


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
    """Bronze transformation for IGN Contours IRIS.

    Reads GeoPackage from landing layer (with original filename preserved) and applies
    basic filtering and geometry conversion. The file is copied to a temp location
    before reading to avoid GDAL file lock contention (see ``_gpkg_safe_read_path``).

    Parameters
    ----------
    landing_path
        Path to the GeoPackage in the landing directory.

    Returns
    -------
    pl.LazyFrame
        Filtered LazyFrame with geometry as WKB, ready for bronze layer.

    Raises
    ------
    duckdb.Error
        On DuckDB spatial query failure (missing extension, corrupt GeoPackage, etc.).
    OSError
        If *landing_path* does not exist or cannot be copied to the temp location.
    """
    logger.debug("Reading GeoPackage from landing")

    with _gpkg_safe_read_path(landing_path) as tmp_gpkg, _duckdb_spatial_conn() as conn:
        # EXCLUDE + re-add as WKB: keeps the column name identical
        # to the source while converting native geometry to binary.
        # * passes new source columns through for drift detection.
        query = """
            SELECT
                * EXCLUDE(geometrie),
                ST_AsWKB(geometrie) AS geometrie
            FROM ST_read(?, layer = 'contours_iris')
        """
        df = _select_to_polars(query, conn=conn, params=[str(tmp_gpkg)])
        logger.debug(
            "DuckDB spatial query completed",
            rows_count=len(df),
            columns_count=len(df.columns),
        )
    return df.lazy()


# --------------------------------------------------------------------------------------
# Silver transformation
# --------------------------------------------------------------------------------------


def transform_silver(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Silver transformation for IGN Contours IRIS.

    Enriches IRIS contours with centroid coordinates in WGS84.
    The original geometry is in Lambert 93 (EPSG:2154), we transform the centroid to
    WGS84 (EPSG:4326) for compatibility with other datasets.

    Collects eagerly — DuckDB spatial operations require a registered DataFrame
    (cannot operate on a LazyFrame).

    Transformations applied:
    - Remove cleabs column (internal IGN identifier, not useful)
    - Compute centroid of each IRIS polygon
    - Transform centroid from Lambert 93 to WGS84
    - Extract latitude and longitude as separate columns

    Parameters
    ----------
    lf
        Pre-processed LazyFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.LazyFrame
        Silver LazyFrame with ``centroid_lat`` and ``centroid_lon`` columns.

    Raises
    ------
    duckdb.Error
        On DuckDB spatial query failure (missing extension, etc.).
    """
    df = collect_narrow(lf)

    # Use DuckDB for spatial operations on the WKB geometry
    with _duckdb_spatial_conn() as conn:
        conn.register("bronze_data", df)

        # geometrie is WKB in Lambert 93 (EPSG:2154). The CTE computes
        # centroid + reprojection once per row; the outer SELECT
        # extracts X/Y.
        query = """
            WITH centroids AS (
                SELECT
                    code_iris,
                    nom_iris,
                    code_insee,
                    nom_commune,
                    type_iris,
                    geometrie,
                    ST_Transform(
                        ST_Centroid(ST_GeomFromWKB(geometrie)),
                        'EPSG:2154',
                        'EPSG:4326'
                    ) AS centroid_wgs84
                FROM bronze_data
            )
            SELECT
                code_iris,
                nom_iris,
                code_insee,
                nom_commune,
                type_iris,
                geometrie,
                -- geometry_always_xy = true (set on the connection) keeps
                -- EPSG:4326 in [lon, lat] order. SilverSchema bounds
                -- (lat 41-52, lon -6 to 10) catch any axis-order
                -- regression from a DuckDB / PROJ upgrade.
                ST_Y(centroid_wgs84) AS centroid_lat,
                ST_X(centroid_wgs84) AS centroid_lon
            FROM centroids
        """

        logger.debug("Computing centroids with DuckDB spatial extension")
        result = _select_to_polars(query, conn=conn)

    logger.debug(
        "Silver transformation completed",
        rows_count=len(result),
        columns_count=len(result.columns),
    )
    return result.lazy()


# --------------------------------------------------------------------------------------
# Transform spec (collected by registry)
# --------------------------------------------------------------------------------------


SPEC = DatasetTransformSpec(
    name="ign_contours_iris",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    primary_key=("code_iris",),
    all_source_columns=_ALL_SOURCE_COLUMNS,
    used_source_columns=_USED_SOURCE_COLUMNS,
    silver_schema=SilverSchema,
)
