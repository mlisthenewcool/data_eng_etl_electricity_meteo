"""Transformations for IGN contours IRIS dataset."""

import shutil
import tempfile
from pathlib import Path

import duckdb
import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings

logger = get_logger("transform.ign_contours_iris")


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for IGN Contours IRIS.

    Reads GeoPackage from landing layer (with original filename preserved) and applies
    basic filtering and geometry conversion.

    The GeoPackage (.gpkg) format is SQLite-based. GDAL (used internally by DuckDB's
    ST_read) acquires file locks when reading, which can cause indefinite hangs in
    Airflow's LocalExecutor if another process holds a lock on the same file. To
    avoid this, the file is copied to a temporary location before reading.

    Parameters
    ----------
    landing_path
        Path to the GeoPackage in the landing directory.

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame with geometry as WKB, ready for bronze layer.

    Raises
    ------
    duckdb.Error
        On DuckDB spatial query failure (missing extension, corrupt GeoPackage, etc.).
    OSError
        If *landing_path* does not exist or cannot be copied to the temp location.
    """
    logger.debug(
        "Reading GeoPackage from landing", landing_path=landing_path, filename=landing_path.name
    )

    # Copy .gpkg to a temp file to avoid SQLite/GDAL file lock contention
    # that causes intermittent hangs in Airflow (LocalExecutor).
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_gpkg = Path(tmp_dir) / landing_path.name
        shutil.copy2(landing_path, tmp_gpkg)
        logger.debug("Copied GeoPackage to temp file", tmp_path=tmp_gpkg)

        with duckdb.connect(":memory:") as conn:
            # INSTALL is idempotent (fast cache check if already present).
            # On Airflow the extension is pre-installed in the Docker image.
            if not settings.is_running_on_airflow:
                conn.execute("INSTALL spatial;")

            conn.execute("LOAD spatial;")
            conn.execute("SET threads = 1")
            conn.execute("SET memory_limit = '1GB'")

            # noinspection SqlResolve
            query = """
        SELECT\
            cleabs, code_insee, nom_commune, iris, code_iris, nom_iris, type_iris,\
            ST_AsWKB(geometrie) AS geom_wkb \
        FROM ST_read(?, layer = 'contours_iris')
        """
            df = conn.execute(query, parameters=[str(tmp_gpkg)]).pl()
            logger.debug("DuckDB spatial query completed", row_count=len(df), columns=df.columns)
    return df


# ---------------------------------------------------------------------------
# Silver transformation
# ---------------------------------------------------------------------------


def transform_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    Enriches IRIS contours with centroid coordinates in WGS84.
    The original geometry is in Lambert 93 (EPSG:2154), we transform the centroid to
    WGS84 (EPSG:4326) for compatibility with other datasets.

    Transformations applied:
    - Remove cleabs column (internal IGN identifier, not useful)
    - Compute centroid of each IRIS polygon
    - Transform centroid from Lambert 93 to WGS84
    - Extract latitude and longitude as separate columns

    Parameters
    ----------
    df
        Pre-processed bronze DataFrame (snake_case columns, all-null columns removed).

    Returns
    -------
    pl.DataFrame
        Silver DataFrame with ``centroid_lat`` and ``centroid_lon`` columns.

    Raises
    ------
    duckdb.Error
        On DuckDB spatial query failure (missing extension, etc.).
    """
    # Use DuckDB for spatial operations on the WKB geometry
    with duckdb.connect(":memory:") as conn:
        # INSTALL is idempotent (fast cache check if already present).
        # On Airflow the extension is pre-installed in the Docker image.
        if not settings.is_running_on_airflow:
            conn.execute("INSTALL spatial;")

        conn.execute("LOAD spatial;")
        conn.execute("SET threads = 1")
        conn.execute("SET memory_limit = '1GB'")

        # Register the pre-processed DataFrame so DuckDB can query it directly
        conn.register("bronze_data", df)

        # Compute centroids and transform to WGS84.
        # geom_wkb is stored as WKB in Lambert 93 (EPSG:2154).
        # The CTE computes ST_GeomFromWKB + ST_Centroid + ST_Transform once per row;
        # the outer SELECT extracts X/Y from the already-transformed point.
        # Note: DuckDB's ST_Transform from Lambert 93 to WGS84 returns coordinates
        # in (lat, lon) order instead of standard (lon, lat), so ST_X gives lat
        # and ST_Y gives lon (counterintuitive but verified empirically).
        # noinspection SqlResolve
        query = """
        WITH centroids AS (
            SELECT
                code_iris,
                nom_iris,
                code_insee,
                nom_commune,
                type_iris,
                geom_wkb,
                ST_Transform(
                    ST_Centroid(ST_GeomFromWKB(geom_wkb)),
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
            geom_wkb,
            ST_X(centroid_wgs84) AS centroid_lat,
            ST_Y(centroid_wgs84) AS centroid_lon
        FROM centroids
        """

        logger.debug("Computing centroids with DuckDB spatial extension")
        result = conn.execute(query).pl()

    logger.debug("Silver transformation completed", row_count=len(result), columns=result.columns)
    return result
