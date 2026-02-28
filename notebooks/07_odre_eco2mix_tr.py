import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from data_eng_etl_electricity_meteo.core.settings import settings

    pl.Config.set_tbl_rows(250)
    pl.Config.set_tbl_cols(10)
    pl.Config.set_tbl_width_chars(500)

    return mo, pl, settings


@app.cell
def _(mo):
    mo.md(r"""
    # ODRE eco2mix Temps Reel

    Exploration of the national real-time electricity production data
    (eco2mix-national-tr). Checks shape, columns, null values, and
    geographic perimeter.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Load data
    """)
    return


@app.cell
def _(pl, settings):
    df = pl.read_parquet(
        settings.data_dir_path / "bronze" / "eco2mix-national-tr_2025.parquet"
    ).sort(by="date_heure", descending=False)
    return (df,)


@app.cell
def _(df):
    print(df.shape)
    print(df.columns)
    return


@app.cell
def _(df):
    print(df.head(20))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data quality checks
    """)
    return


@app.cell
def _(df, pl):
    # All rows should be "France" perimeter (national dataset)
    n_non_france = df.filter(pl.col("perimetre") != "France").shape[0]
    print(f"rows with perimetre != 'France': {n_non_france}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Null analysis
    """)
    return


@app.cell
def _(df, pl):
    # Show null counts only for columns that actually have nulls
    null_counts = (
        df.select(pl.all().null_count())
        .unpivot(variable_name="column", value_name="null_count")
        .filter(pl.col("null_count") > 0)
        .sort("null_count", descending=True)
    )

    print(f"{null_counts.shape[0]} columns with nulls out of {len(df.columns)}")
    null_counts
    return


@app.cell
def _(df, pl):
    # Temporal range
    print(f"min date_heure: {df.select(pl.col('date_heure').min()).item()}")
    print(f"max date_heure: {df.select(pl.col('date_heure').max()).item()}")
    print(f"total rows: {df.shape[0]:_}")
    return


if __name__ == "__main__":
    app.run()
