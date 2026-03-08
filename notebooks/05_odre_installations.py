import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from data_eng_etl_electricity_meteo.core.data_catalog import (
        DataCatalog,
    )
    from data_eng_etl_electricity_meteo.core.exceptions import (
        InvalidCatalogError,
    )
    from data_eng_etl_electricity_meteo.core.logger import get_logger
    from data_eng_etl_electricity_meteo.core.settings import settings
    from data_eng_etl_electricity_meteo.pipeline.path_resolver import (
        RemotePathResolver,
    )

    logger = get_logger("notebook")

    return (
        DataCatalog,
        InvalidCatalogError,
        RemotePathResolver,
        logger,
        mo,
        pl,
        settings,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # ODRE Installations

    Exploration of the national registry of electricity production and
    storage installations (registre national). Covers data quality,
    geographic coverage, power capacity analysis, and comparison with
    RTE published figures.
    """)
    return


# ------------------------------------------------------------------ #
#  Setup & loading                                                     #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Setup & loading
    """)
    return


@app.cell
def _(
    DataCatalog,
    InvalidCatalogError,
    RemotePathResolver,
    logger,
    mo,
    settings,
):
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
        dataset = catalog.get_remote_dataset("odre_installations")
        path_resolver = RemotePathResolver(dataset_name=dataset.name)

        logger.info("Configuration terminée")
        logger.info(
            "Dataset configuré",
            **dataset.model_dump(exclude_none=True),
        )
    except InvalidCatalogError as e:
        logger.exception(
            "Erreur de configuration",
            **(e.validation_errors or {}),
        )

        feedback = mo.vstack(
            [
                mo.md("### Configuration error!"),
                mo.md(str(e)),
                mo.tree(e.validation_errors),
            ]
        ).callout(kind="danger")

        mo.stop(True, feedback)
    return (path_resolver,)


@app.cell
def _(path_resolver, pl):
    df = pl.read_parquet(path_resolver.silver_current_path)
    return (df,)


@app.cell
def _(df, mo):
    mo.ui.dataframe(df)
    return


# ------------------------------------------------------------------ #
#  Data quality                                                        #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Data quality — primary key checks
    """)
    return


@app.cell
def _(df, pl):
    pk_cols = ["idpeps", "codeeicresourceobject"]

    def check_duplicated_on_pk(cols: list[str]):
        for col in cols:
            print(
                f"duplicated rows on '{col}':",
                df.filter(pl.col(col).is_not_null() & pl.col(col).is_duplicated()).shape[0],
            )

    check_duplicated_on_pk(cols=pk_cols)
    return


@app.cell
def _(df):
    df.null_count()
    return


@app.cell
def _(df, pl):
    # check datederaccordement
    df.filter(pl.col("datederaccordement").is_not_null())
    return


@app.cell
def _(df, pl):
    # datemiseenservice vs datedebutversion
    df.filter(
        pl.col("datedebutversion").is_not_null()
        & pl.col("datemiseenservice").ne_missing(other=pl.col("datedebutversion"))
    )
    return


@app.cell
def _(df, pl):
    # Suspicious early dates (before 1905)
    df.filter(pl.col("datemiseenservice_date") < pl.date(1905, 1, 1))
    return


# ------------------------------------------------------------------ #
#  Geography                                                           #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Geography — IRIS / INSEE / EPCI coverage
    """)
    return


@app.cell
def _(df, pl):
    geography_cols = [
        "codeiris",
        "codeinseecommune",
        "codeepci",
        "codedepartement",
        "coderegion",
    ]

    print(df.select(pl.col(geography_cols).null_count()))

    # Rows with no geographic data at all
    df.filter(pl.all_horizontal(pl.col(geography_cols).is_null()))
    return


@app.cell
def _(df, mo, pl):
    # 1306 rows without IRIS → aggregated installations, safe to exclude
    mo.ui.dataframe(
        df.filter(pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null()))
    )

    # Confirm: only aggregated names
    df.filter(
        pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null())
    ).select(pl.col("nominstallation").unique())
    return


@app.cell
def _(df, pl):
    # Rows with no IRIS and no INSEE commune
    df.filter(pl.all_horizontal(pl.col(["codeiris", "codeinseecommune"]).is_null()))
    return


@app.cell
def _(df, pl):
    # Verify IRIS→INSEE consistency: same commune should map to same EPCI
    _df = df.group_by(pl.col(["codeiris", "codeinseecommune"])).agg(
        pl.col("epci").n_unique().alias("n_unique_epci")
    )
    _df.filter(pl.col("n_unique_epci") > 1)
    return


@app.cell
def _(df, pl):
    # Hierarchical geographic breakdown by best-available level
    print("df (shape)   => ", df.shape)
    print(f"df (maxpuis) => {df.select(pl.col('maxpuis').sum().round(2)).item():_}")
    print()

    _df_iris = df.filter(pl.col("codeiris").is_not_null())
    print("with codeiris (shape)   => ", _df_iris.shape)
    print(
        f"with codeiris (maxpuis) => {_df_iris.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_insee = df.filter(pl.col("codeiris").is_null() & pl.col("codeinseecommune").is_not_null())
    print(
        "with codeinseecommune only (shape)   => ",
        _df_insee.shape,
    )
    print(
        "with codeinseecommune only (maxpuis) => "
        f"{_df_insee.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_epci = df.filter(
        pl.col("codeiris").is_null()
        & pl.col("codeinseecommune").is_null()
        & pl.col("codeepci").is_not_null()
    )
    print("with codeepci only (shape)   => ", _df_epci.shape)
    print(
        "with codeepci only (maxpuis) => "
        f"{_df_epci.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_dept = df.filter(
        pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null())
        & pl.col("codedepartement").is_not_null()
    )
    print(
        "with codedepartement only (shape)   => ",
        _df_dept.shape,
    )
    print(
        "with codedepartement only (maxpuis) => "
        f"{_df_dept.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_region = df.filter(
        pl.all_horizontal(
            pl.col(
                [
                    "codeiris",
                    "codeinseecommune",
                    "codeepci",
                    "codedepartement",
                ]
            ).is_null()
        )
        & pl.col("coderegion").is_not_null()
    )
    print(
        "with coderegion only (shape)   => ",
        _df_region.shape,
    )
    print(
        "with coderegion only (maxpuis) => "
        f"{_df_region.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_none = df.filter(
        pl.all_horizontal(
            pl.col(
                [
                    "codeiris",
                    "codeinseecommune",
                    "codeepci",
                    "codedepartement",
                    "coderegion",
                ]
            ).is_null()
        )
    )
    print("with no geo data (shape)   => ", _df_none.shape)
    print(
        "with no geo data (maxpuis) => "
        f"{_df_none.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    return


@app.cell
def _(df, pl):
    # Corsica is covered by RTE but has specific grid conditions (island)
    df.filter(pl.col("region") == "Corse")
    return


@app.cell
def _(df, pl):
    # Coherence: IRIS implies INSEE commune (should be 0)
    print(df.filter(pl.col("codeiris").is_not_null() & pl.col("codeinseecommune").is_null()).shape)
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeepci").is_null())
    return


@app.cell
def _(df, pl):
    # INSEE commune null but EPCI present
    df.filter(pl.col("codeinseecommune").is_null() & pl.col("codeepci").is_not_null())
    return


# ------------------------------------------------------------------ #
#  Power capacity                                                      #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Power capacity — column comparison and analysis
    """)
    return


@app.cell
def _(df, pl):
    # puismaxinstallee vs maxpuis — should be equal
    df.select(pl.col("puismaxinstallee")).to_series().equals(
        other=df.select(pl.col("maxpuis")).to_series()
    )
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("puismaxinstallee").ne_missing(other=pl.col("maxpuis")))
    return


@app.cell
def _(df, pl):
    for col in ["puismaxinstallee", "maxpuis"]:
        print(col, df.select(pl.col(col).sum()).item())
    return


@app.cell
def _(df, pl):
    # All power-related columns
    df.select(pl.selectors.contains("puis").sum().round(decimals=3))
    return


@app.cell
def _(df, pl):
    # All energy-related columns
    df.select(pl.selectors.contains("energie").sum().round(decimals=3))
    return


@app.cell
def _():
    columns_numeric = [
        "puismaxinstallee",
        "puismaxraccharge",
        "puismaxcharge",
        "puismaxrac",
        "puismaxinstalleedischarge",
        "energiestockable",
        "capacitereservoir",
        "hauteurchute",
        "productible",
        "debitmaximal",
        "energieannuelleglissanteinjectee",
        "energieannuelleglissanteproduite",
        "energieannuelleglissantesoutiree",
        "energieannuelleglissantestockee",
        "maxpuis",
    ]
    return (columns_numeric,)


@app.cell
def _(columns_numeric, df, pl):
    df.select([pl.col(column).sum() for column in columns_numeric])
    return


@app.cell
def _(df, pl):
    # Breakdown by filiere
    df.group_by("filiere").agg(pl.col("nbinstallations").sum())
    return


# ------------------------------------------------------------------ #
#  RTE reference comparison                                            #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## RTE reference — comparison with published figures

    Compare installed power capacity against figures published on the
    [RTE eco2mix website](https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel/chiffres-cles-electricite#parc-France).
    """)
    return


@app.cell
def _():
    repartitions_site_internet_rte = {
        "charbon": 1810,
        "bioenergies": 2277,
        "fioul": 2623,
        "gaz": 12462,
        "eolien": 25512,
        "hydraulique": 25524,
        "solaire": 28761,
        "nucleaire": 62990,
    }

    print(
        "puissance max installee selon le site de RTE",
        sum(repartitions_site_internet_rte.values()),
    )
    return


@app.cell
def _():
    regions_hors_scope_site_internet_rte = [
        "Martinique",
        "Guadeloupe",
        "Guyane",
        "La Réunion",
    ]
    return (regions_hors_scope_site_internet_rte,)


@app.cell
def _(df, pl, regions_hors_scope_site_internet_rte):
    df_sans_dom_tom = df.filter(~pl.col("region").is_in(regions_hors_scope_site_internet_rte))
    df_sans_dom_tom_2025 = df_sans_dom_tom.filter(pl.col("datemiseenservice").str.ends_with("2025"))

    puismaxinstallee_2025 = df_sans_dom_tom_2025.select(pl.col("puismaxinstallee").sum()).item()

    puismaxinstallee_total = df_sans_dom_tom.select(pl.col("puismaxinstallee").sum()).item()
    puismaxcharge_total = df_sans_dom_tom.select(pl.col("puismaxcharge").sum()).item()
    puismaxraccharge_total = df_sans_dom_tom.select(pl.col("puismaxraccharge").sum()).item()

    # Compute "en retrait provisoire" power dynamically instead of hardcoding
    puismaxinstallee_en_retrait = (
        df_sans_dom_tom.filter(pl.col("regime") == "En retrait provisoire")
        .select(pl.col("puismaxinstallee").sum())
        .item()
    )

    # Compute aggregation power dynamically
    puismaxinstallee_aggregations = (
        df_sans_dom_tom.filter(pl.col("codeeicresourceobject").is_null())
        .select(pl.col("puismaxinstallee").sum())
        .item()
    )

    print(f"puismaxinstallee                              => {puismaxinstallee_total:_.2f}")
    print(
        f"puismaxinstallee - puismaxcharge               => "
        f"{puismaxinstallee_total - puismaxcharge_total:_.2f}"
    )
    print(
        f"  - en retrait provisoire                      => "
        f"{puismaxinstallee_total - puismaxcharge_total - puismaxinstallee_en_retrait:_.2f}"
    )
    print(
        f"puismaxinstallee - aggregations                => "
        f"{puismaxinstallee_total - puismaxinstallee_aggregations:_.2f}"
    )
    print(
        f"puismaxinstallee - mise en service 2025        => "
        f"{puismaxinstallee_total - puismaxinstallee_2025:_.2f}"
    )
    print(
        f"puismaxinstallee - puismaxcharge - raccharge   => "
        f"{puismaxinstallee_total - puismaxcharge_total - puismaxraccharge_total:_.2f}"
    )
    return (df_sans_dom_tom,)


@app.cell
def _(df, pl):
    # Same analysis including DOM-TOM
    puismaxinstallee_totale = df.select(pl.col("puismaxinstallee").sum()).item()
    puismaxcharge_totale = df.select(pl.col("puismaxcharge").sum()).item()
    puismaxinstallee_en_retrait_all = (
        df.filter(pl.col("regime") == "En retrait provisoire")
        .select(pl.col("puismaxinstallee").sum())
        .item()
    )

    print(f"[all regions] puismaxinstallee             => {puismaxinstallee_totale:_.2f}")
    print(
        f"[all regions] - puismaxcharge              => "
        f"{puismaxinstallee_totale - puismaxcharge_totale:_.2f}"
    )
    print(
        f"[all regions] - charge - en retrait prov.  => "
        f"{puismaxinstallee_totale - puismaxcharge_totale - puismaxinstallee_en_retrait_all:_.2f}"
    )
    return


@app.cell
def _(df, pl):
    # Status regime breakdown
    print(df.select(pl.col("regime").value_counts()))

    df.filter(pl.col("regime") == "En retrait provisoire").select(pl.col("puismaxinstallee").sum())
    return


@app.cell
def _(df_sans_dom_tom, pl):
    # Photovoltaic subset
    df_sans_dom_tom.filter(pl.col("technologie") == "Photovoltaïque").select(
        [
            pl.col("nbinstallations").sum(),
            pl.col("puismaxinstallee").sum(),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Filiere breakdown — excluding aggregations
    """)
    return


@app.cell
def _(df_sans_dom_tom, pl):
    # Exclude aggregated rows (no EIC resource code)
    df_sans_dom_tom_ni_aggregations = df_sans_dom_tom.filter(
        pl.col("codeeicresourceobject").is_not_null()
    )

    puismaxinstallee_sans_agg = df_sans_dom_tom_ni_aggregations.select(
        pl.col("puismaxinstallee").sum()
    ).item()

    df_sans_dom_tom_ni_aggregations.group_by("filiere").agg(
        pl.col("puismaxinstallee").sum(),
        (pl.col("puismaxinstallee").sum() / puismaxinstallee_sans_agg * 100)
        .round(2)
        .alias("puismaxinstalle_pourcentage"),
    ).sort(by="puismaxinstallee", descending=True)
    return (df_sans_dom_tom_ni_aggregations,)


@app.cell
def _(df, pl):
    # Same analysis but including DOM-TOM
    df_sans_aggregations = df.filter(pl.col("codeeicresourceobject") != "")

    puismaxinstallee_sans_agg = df_sans_aggregations.select(pl.col("puismaxinstallee").sum()).item()

    df_sans_aggregations.group_by("filiere").agg(
        pl.col("puismaxinstallee").sum(),
        (pl.col("puismaxinstallee").sum() / puismaxinstallee_sans_agg * 100)
        .round(2)
        .alias("puismaxinstalle_pourcentage"),
    ).sort(by="puismaxinstallee", descending=True)
    return


# ------------------------------------------------------------------ #
#  Aggregations analysis                                               #
# ------------------------------------------------------------------ #


@app.cell
def _(mo):
    mo.md(r"""
    ## Aggregations — multi-installation rows
    """)
    return


@app.cell
def _(df, pl):
    df_aggregations = df.filter(pl.col("nbinstallations") > 1)
    return (df_aggregations,)


@app.cell
def _(df_aggregations, pl):
    df_aggregations.select(pl.col("codeiris")).null_count()
    return


@app.cell
def _(df, pl):
    df.select(pl.col("codeiris")).null_count()
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeiris").is_null())
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeiris").is_null()).select(pl.col("puismaxinstallee").sum())
    return


@app.cell
def _(df, pl):
    # Power by technology for rows without IRIS code
    df.filter(pl.col("codeiris").is_null()).group_by("technologie").agg(
        pl.col("puismaxinstallee").sum()
    ).sort(by="puismaxinstallee", descending=True)
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeiris").is_null() & pl.col("codeinseecommune").is_null())
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeinseecommune").is_null())
    return


@app.cell
def _(df_sans_dom_tom, pl):
    # Aggregated rows: power by technology
    df_sans_dom_tom.filter(pl.col("codeeicresourceobject").is_null()).group_by("technologie").agg(
        pl.col("puismaxinstallee").sum(),
        pl.col("puismaxcharge").sum(),
    ).sort(by="puismaxinstallee", descending=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Regional breakdown
    """)
    return


@app.cell
def _(df_sans_dom_tom, pl):
    df_sans_dom_tom.group_by("region").agg(
        pl.col("puismaxinstallee").sum(),
        pl.col("puismaxcharge").sum(),
    ).sort(by="puismaxinstallee", descending=True)
    return


@app.cell
def _(df_sans_dom_tom_ni_aggregations, pl):
    df_sans_dom_tom_ni_aggregations.group_by("region").agg(
        pl.col("puismaxinstallee").sum(),
        pl.col("puismaxcharge").sum(),
    ).sort(by="puismaxinstallee", descending=True)
    return


@app.cell
def _(df_sans_dom_tom, pl):
    # Regional breakdown excluding "En retrait provisoire"
    (
        df_sans_dom_tom.filter(pl.col("regime") != "En retrait provisoire")
        .group_by("region")
        .agg(
            pl.col("puismaxinstallee").sum(),
            pl.col("puismaxcharge").sum(),
        )
        .sort(by="puismaxinstallee", descending=True)
    )
    return


if __name__ == "__main__":
    app.run()
