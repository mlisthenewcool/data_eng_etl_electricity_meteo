import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    from calendar import isleap

    import marimo as mo
    import polars as pl

    from data_eng_etl_electricity_meteo.core.data_catalog import (
        DataCatalog,
    )
    from data_eng_etl_electricity_meteo.core.settings import settings
    from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
        RemotePathResolver,
    )

    return (
        DataCatalog,
        RemotePathResolver,
        isleap,
        mo,
        pl,
        settings,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # ODRE eco2mix Consommation Definitive

    Exploration of the regional definitive electricity consumption data
    (eco2mix-regional-cons-def). Compares data from OpenDataSoft and
    data.gouv.fr, then analyzes temporal coverage and data nature
    (definitive vs consolidated).

    ## ODRE API query pattern (for future delta fetch)

    The OpenDataSoft API supports datetime filtering via `where` clauses,
    which could be used for incremental fetching instead of full downloads:

    ```
    GET https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/{dataset}/exports/json
    params:
      where: "date_heure >= '2026-01-02T10:00:00Z' AND date_heure <= '2026-01-02T10:59:59Z'"
      timezone: "Europe/Berlin"
      limit: -1
    ```
    """)
    return


# ------------------------------------------------------------------ #
#  Setup & loading from pipeline                                       #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Load from pipeline (bronze)
    """)
    return


@app.cell
def _(DataCatalog, settings):
    catalog = DataCatalog.load(settings.data_catalog_file_path)
    dataset = catalog.get_remote_dataset("odre_eco2mix_cons_def")
    return (dataset,)


@app.cell
def _(RemotePathResolver, dataset, pl):
    df = pl.read_parquet(RemotePathResolver(dataset_name=dataset.name).bronze_latest_path)
    return (df,)


@app.cell
def _(df):
    print(df.columns)
    print(df.height)
    return


@app.cell
def _(df):
    # column_30 is always empty (artifact from CSV export)
    df_clean = df.drop(["column_30"])
    return (df_clean,)


@app.cell
def _(df_clean):
    print(df_clean.columns)
    return


# ------------------------------------------------------------------ #
#  Source comparison: OpenDataSoft vs data.gouv.fr                      #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Source comparison — OpenDataSoft vs data.gouv.fr

    Verify that both sources contain equivalent data
    (ignoring the "Date - Heure" formatting differences).
    """)
    return


@app.cell
def _(pl, settings):
    df_opendatasoft = pl.read_csv(
        settings.data_dir_path / "landing" / "eco2mix-regional-cons-def.csv",
        separator=";",
    ).drop(["Column 30"])
    df_datagouv = pl.read_csv(
        settings.data_dir_path / "landing" / "from_datagouv_eco2mix-regional-cons-def.csv",
        separator=";",
    ).drop(["Column 30"])

    df_opendatasoft_sans_dateheure = df_opendatasoft.drop(["Date - Heure"])
    df_datagouv_sans_dateheure = df_datagouv.drop(["Date - Heure"])

    df_opendatasoft_sans_dateheure.equals(other=df_datagouv_sans_dateheure, null_equal=True)
    return df_datagouv, df_opendatasoft


@app.cell
def _(df_datagouv, df_opendatasoft, pl):
    df_opendatasoft_def = df_opendatasoft.filter(pl.col("Nature") == "Données définitives")
    df_datagouv_def = df_datagouv.filter(pl.col("Nature") == "Données définitives")
    return df_datagouv_def, df_opendatasoft_def


@app.cell
def _(df_datagouv_def):
    df_datagouv_def
    return


@app.cell
def _(df_opendatasoft_def):
    df_opendatasoft_def
    return


# ------------------------------------------------------------------ #
#  Historical analysis (from eco2mix regional parquet)                  #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Historical analysis — nature, temporal coverage, regions

    Analyze the full eco2mix regional parquet to understand data nature
    (definitive vs consolidated vs real-time) and temporal ranges.
    """)
    return


@app.cell
def _(pl, settings):
    df_historique = pl.read_parquet(
        settings.data_dir_path / "bronze" / "eco2mix-regional-cons-def.parquet"
    ).sort(by="date_heure", descending=False)
    return (df_historique,)


@app.cell
def _(df_historique):
    print(df_historique.shape)
    print(df_historique.columns)
    return


@app.cell
def _(df_historique, pl):
    # column_30 is always null
    print(df_historique.filter(pl.col("column_30").is_not_null()).select(pl.col("column_30")))
    return


@app.cell
def _(df_historique, pl):
    # TCO columns null analysis
    print(
        df_historique.select(
            pl.col([col for col in df_historique.columns if "tco" in col])
        ).null_count()
    )
    return


@app.cell
def _(df_historique, pl):
    # Temporal range
    print(df_historique.select(pl.col("date_heure")).min())
    print(df_historique.select(pl.col("date_heure")).max())
    return


@app.cell
def _(df_historique, pl):
    # Nature breakdown
    print(df_historique.filter(pl.col("nature") == "Données consolidées").count())
    print(df_historique.filter(pl.col("nature") == "Données définitives").count())
    return


@app.cell
def _(df_historique, pl):
    df_donnees_consolidees = df_historique.filter(pl.col("nature") == "Données consolidées")
    print(df_donnees_consolidees.shape)
    print(df_donnees_consolidees.select(pl.col("date_heure")).min())
    print(df_donnees_consolidees.select(pl.col("date_heure")).max())
    return (df_donnees_consolidees,)


@app.cell
def _(df_historique, pl):
    df_donnees_definitives = df_historique.filter(pl.col("nature") == "Données définitives")
    print(df_donnees_definitives.shape)
    print(df_donnees_definitives.select(pl.col("date_heure")).min())
    print(df_donnees_definitives.select(pl.col("date_heure")).max())
    return (df_donnees_definitives,)


@app.cell
def _(
    df_donnees_consolidees,
    df_donnees_definitives,
    df_historique,
):
    assert len(df_historique) == len(df_donnees_consolidees) + len(df_donnees_definitives), (
        "total rows != consolidated + definitive — unexpected data nature values"
    )
    # données définitives  => 2013-01-01 -> 2023-12-31
    # données consolidées  => 2024-01-01 -> 2024-12-31
    # données temps réel   => 2025-01-01 -> today
    return


@app.cell
def _(df_historique, pl):
    # Number of distinct regions
    print(df_historique.select(pl.col("libelle_region").unique().count()))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Expected vs actual row count
    """)
    return


@app.cell
def _(df_donnees_definitives, isleap):
    nombre_demie_heures_dans_1_jour = 48
    nombre_regions = 12  # pas la Corse ni les DOM/TOM
    nombre_total_jours = sum(366 if isleap(annee) else 365 for annee in range(2013, 2024))

    n = nombre_demie_heures_dans_1_jour * nombre_regions * nombre_total_jours
    # Difference = -4032, some rows are missing
    print(len(df_donnees_definitives) - n)
    return


if __name__ == "__main__":
    app.run()
