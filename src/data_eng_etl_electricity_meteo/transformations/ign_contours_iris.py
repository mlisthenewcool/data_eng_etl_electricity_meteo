"""Transformations for IGN contours IRIS dataset."""

import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import duckdb
import polars as pl

from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column, DataFrameModel
from data_eng_etl_electricity_meteo.transformations.shared import validate_source_columns
from data_eng_etl_electricity_meteo.transformations.spec import DatasetTransformSpec

logger = get_logger("transform.ign_contours_iris")


@contextmanager
def _duckdb_spatial_conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB in-memory connection with the spatial extension loaded.

    On Airflow the extension is pre-installed in the Docker image, so ``INSTALL`` is
    skipped to avoid unnecessary network/disk checks.
    """
    with duckdb.connect(":memory:") as conn:
        if not settings.is_running_on_airflow:
            conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        conn.execute("SET threads = 1")
        conn.execute("SET memory_limit = '1GB'")
        yield conn


# --------------------------------------------------------------------------------------
# Silver schema
# --------------------------------------------------------------------------------------


_ALL_SOURCE_COLUMNS: set[str] = {
    "cleabs",
    "code_insee",
    "code_iris",
    "geom_wkb",
    "iris",
    "nom_commune",
    "nom_iris",
    "type_iris",
}

# cleabs (internal IGN id) and iris (redundant with code_iris) are not used.
_USED_SOURCE_COLUMNS: set[str] = _ALL_SOURCE_COLUMNS - {"cleabs", "iris"}


class SilverSchema(DataFrameModel):
    """Silver output contract for IGN contours IRIS."""

    code_iris: Annotated[str, Column(nullable=False, unique=True)]
    nom_iris: str
    code_insee: str
    nom_commune: str
    type_iris: Annotated[str, Column(isin=["H", "A", "D", "Z"])]
    geom_wkb: Annotated[bytes, Column(dtype=pl.Binary(), nullable=False)]
    # Metropolitan France only (overseas territories excluded at source level)
    centroid_lat: Annotated[float, Column(nullable=False, ge=41.0, le=52.0)]
    centroid_lon: Annotated[float, Column(nullable=False, ge=-6.0, le=10.0)]


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


def transform_bronze(landing_path: Path) -> pl.LazyFrame:
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

    # Copy .gpkg to a temp file to avoid SQLite/GDAL file lock contention
    # that causes intermittent hangs in Airflow (LocalExecutor).
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_gpkg = Path(tmp_dir) / landing_path.name
        shutil.copy2(landing_path, tmp_gpkg)
        logger.debug("Copied GeoPackage to temp file", tmp_path=tmp_gpkg)

        with _duckdb_spatial_conn() as conn:
            # noinspection SqlResolve
            query = """
        SELECT\
            cleabs, code_insee, nom_commune, iris, code_iris, nom_iris, type_iris,\
            ST_AsWKB(geometrie) AS geom_wkb \
        FROM ST_read(?, layer = 'contours_iris')
        """
            df = conn.execute(query, parameters=[str(tmp_gpkg)]).pl()
            logger.debug(
                "DuckDB spatial query completed",
                rows_count=len(df),
                columns_count=len(df.columns),
            )
    return df.lazy()


# --------------------------------------------------------------------------------------
# Silver transformation
# --------------------------------------------------------------------------------------


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
    validate_source_columns(df, _ALL_SOURCE_COLUMNS, "ign_contours_iris")

    # Use DuckDB for spatial operations on the WKB geometry
    with _duckdb_spatial_conn() as conn:
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

    logger.debug(
        "Silver transformation completed",
        rows_count=len(result),
        columns_count=len(result.columns),
    )
    SilverSchema.validate(result)
    return result


# --------------------------------------------------------------------------------------
# Transform spec (collected by registry)
# --------------------------------------------------------------------------------------


SPEC = DatasetTransformSpec(
    name="ign_contours_iris",
    bronze_transform=transform_bronze,
    silver_transform=transform_silver,
    all_source_columns=frozenset(_ALL_SOURCE_COLUMNS),
    used_source_columns=frozenset(_USED_SOURCE_COLUMNS),
    silver_schema=SilverSchema,
)
