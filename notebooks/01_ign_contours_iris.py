import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl

    from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
        RemotePathResolver,
    )

    return RemotePathResolver, duckdb, mo, pl


@app.cell
def _(mo):
    mo.md(r"""
    # IGN Contours IRIS

    Exploration of the IGN IRIS geographical boundaries using DuckDB spatial
    extension. IRIS (Ilots Regroupés pour l'Information Statistique) are the
    finest-grained INSEE statistical units (~2000 inhabitants each).
    """)
    return


@app.cell
def _(RemotePathResolver):
    # The GeoPackage only lives in landing during ingestion; bronze keeps the
    # contours_iris layer as Parquet, geometry as WKB.
    contours_iris_path = RemotePathResolver(dataset_name="ign_contours_iris").bronze_latest_path
    return (contours_iris_path,)


@app.cell
def _(duckdb, pl):
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    pl.Config.set_tbl_width_chars(width=300)
    pl.Config.set_tbl_rows(n=30)
    return (con,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Read IRIS geometries
    """)
    return


@app.cell
def _(con, contours_iris_path, pl):
    query = """
        SELECT *, ST_AsGeoJSON(ST_GeomFromWKB(geometrie))
        FROM read_parquet(?)
    """
    iris = con.sql(query, params=[str(contours_iris_path)]).pl()

    print(f"shape: {iris.shape}")
    print(f"columns: {iris.columns}")
    iris.sort(by="code_iris")
    return (iris,)


@app.cell
def _(iris, pl):
    # Spot-check: commune 01014 should have multiple IRIS codes
    iris.filter(pl.col("code_insee") == "01014")
    return


if __name__ == "__main__":
    app.run()
