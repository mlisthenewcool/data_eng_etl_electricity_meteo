import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from data_eng_etl_electricity_meteo.core.settings import settings

    bronze_path = settings.data_dir_path / "bronze"

    return bronze_path, mo, pl


@app.cell
def _(mo):
    mo.md(r"""
    # Meteo France Climatologie

    Exploration of historical climatological data from Meteo France.
    Reads CSV files for specific weather stations, parses dates, and
    verifies temporal coverage.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Station-level climatology data
    """)
    return


@app.cell
def _(bronze_path, pl):
    station_ids = ["01071001"]  # "01089001", "01014002"

    paths = {
        sid: (bronze_path / "meteo_france_climatologie" / f"_station_{sid}_*.csv")
        for sid in station_ids
    }

    dfs = {
        sid: pl.read_csv(
            paths[sid],
            has_header=True,
            separator=";",
            schema_overrides={
                "DATE": pl.String,
                "POSTE": pl.String,
            },
        )
        for sid in station_ids
    }
    return dfs, station_ids


@app.cell
def _(dfs, station_ids):
    _df = dfs[station_ids[0]]
    print(f"station {station_ids[0]}: {_df.shape[0]} rows, {len(_df.columns)} columns")
    return


@app.cell
def _(dfs, pl, station_ids):
    # Pad DATE with "00" for minute field, then parse as datetime
    dfs_date_cleaned = {
        sid: (
            dfs[sid].with_columns(
                pl.col("DATE")
                .str.strip_chars()
                .add("00")
                .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
            )
        )
        for sid in station_ids
    }
    return (dfs_date_cleaned,)


@app.cell
def _(dfs_date_cleaned, station_ids):
    dfs_date_cleaned[station_ids[0]]
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Department-level climatology (from eco2mix analysis)

    Historical hourly climatological data for department 13
    (Bouches-du-Rhone), 2020-2025.
    """)
    return


@app.cell
def _(bronze_path, pl):
    df_clim_13 = pl.read_csv(bronze_path / "clim-base_hor-13-2020-2025.csv")
    return (df_clim_13,)


@app.cell
def _(df_clim_13):
    print(df_clim_13.shape)
    print(df_clim_13.head(10))
    return


@app.cell
def _(df_clim_13, pl):
    # Verify temporal coverage
    print(df_clim_13.select(pl.col("aaaammjjhh").min()))
    print(df_clim_13.select(pl.col("aaaammjjhh").max()))
    return


if __name__ == "__main__":
    app.run()
