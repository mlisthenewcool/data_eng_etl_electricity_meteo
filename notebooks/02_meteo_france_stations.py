import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from data_eng_etl_electricity_meteo.core.settings import settings

    bronze_path = settings.data_dir_path / "bronze"

    return bronze_path, mo, pl, settings


@app.cell
def _(mo):
    mo.md(r"""
    # Meteo France Stations

    Exploration and filtering of Meteo France weather stations metadata.
    Identifies active stations with relevant meteorological parameters
    for renewable energy analysis (solar PV + wind).
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Load raw station data
    """)
    return


@app.cell
def _(pl, settings):
    df = pl.read_json(
        settings.data_dir_path
        / "0_pour_investigations"
        / "meteo_france_info_stations_depuis_data_gouv.json"
    )
    return (df,)


@app.cell
def _(df):
    print(df.shape)
    print(df.columns)
    return


@app.cell
def _(df):
    df.head(10)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Filter active stations
    """)
    return


@app.cell
def _(df, pl):
    df_with_only_active_stations = df.filter(
        pl.col("dateFin").is_null() | (pl.col("dateFin") == "")
    )
    df_with_only_active_stations  # 2397 active stations
    return (df_with_only_active_stations,)


@app.cell
def _(df_with_only_active_stations, pl):
    # Count active type-0 postes per station
    df_with_type0_count = df_with_only_active_stations.with_columns(
        pl.col("typesPoste")
        .list.eval(
            (pl.element().struct.field("type") == 0)
            & (
                pl.element().struct.field("dateFin").is_null()
                | (pl.element().struct.field("dateFin") == "")
            )
        )
        .list.sum()
        .alias("n_postes_actifs_type_0")
    )

    print(
        "type 0 stations with >1 active poste (should be 0):",
        df_with_type0_count.filter(pl.col("n_postes_actifs_type_0") > 1).shape,
    )

    df_with_only_active_stations_and_type_0 = df_with_type0_count.filter(
        pl.col("n_postes_actifs_type_0") == 1
    )
    return (df_with_only_active_stations_and_type_0,)


@app.cell
def _(df_with_only_active_stations_and_type_0):
    df_with_only_active_stations_and_type_0
    return


@app.cell
def _(df_with_only_active_stations_and_type_0, pl):
    condition_exclusion = (pl.element().struct.field("dateFin").is_not_null()) & (
        pl.element().struct.field("dateFin") != ""
    )

    df_active_type0_with_active_params = df_with_only_active_stations_and_type_0.with_columns(
        pl.col("parametres")
        .list.eval(pl.element().filter(~condition_exclusion))
        .alias("parametres_actifs")
    )

    df_active_type0_with_active_params
    return (df_active_type0_with_active_params,)


@app.cell
def _(df_active_type0_with_active_params, pl):
    print(df_active_type0_with_active_params.select(pl.col("parametres_actifs").list.len().max()))
    print(df_active_type0_with_active_params.select(pl.col("parametres").list.len().max()))
    return


@app.cell
def _(df_active_type0_with_active_params, pl):
    liste_parametres_actifs = (
        df_active_type0_with_active_params.select(
            pl.col("parametres_actifs").list.eval(pl.element().struct.field("nom"))
        )
        .explode("parametres_actifs")
        .unique()
    )["parametres_actifs"].to_list()

    liste_parametres_actifs
    return (liste_parametres_actifs,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Parameter selection v1 — renewable energy relevance

    Manually curated dict mapping every known Meteo France parameter
    to True (relevant for solar/wind analysis) or False.
    """)
    return


@app.cell
def _():
    selection_meteo_renouvelables = {
        # --- SOLAIRE PHOTOVOLTAÏQUE (PV) ---
        "RAYONNEMENT GLOBAL HORAIRE": True,
        "RAYONNEMENT GLOBAL QUOTIDIEN": True,
        "RAYONNEMENT DIRECT HORAIRE": True,
        "DUREE D'INSOLATION HORAIRE": True,
        "NEBULOSITE TOTALE HORAIRE": True,
        "TEMPERATURE SOUS ABRI HORAIRE": True,
        "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE": True,
        # --- ÉOLIEN ---
        "VITESSE DU VENT HORAIRE": True,
        "MOYENNE DES VITESSES DU VENT A 10M": True,
        "DIRECTION DU VENT A 10 M HORAIRE": True,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES": True,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES": True,
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": True,
        "NOMBRE DE JOURS AVEC FXY>=8 M/S": True,
        # --- AUTRES (False par défaut) ---
        "HEURE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MAXI": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 6°C": False,
        "NOMBRE DE JOURS AVEC FXY>=15 M/S": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "NOMBRE DE JOURS AVEC TX<=20°C": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 200 MM": False,
        "HEURE DU TX SOUS ABRI HORAIRE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 50 MM": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MINI": False,
        "DIRECTION DU VENT MOYEN SUR 10 MN MAXIMAL HORAIRE": False,
        "DUREE HUMECTATION QUOTIDIENNE": False,
        "BASE DE LA 2EME COUCHE NUAGEUSE": False,
        "VITESSE VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "DUREE DES PRECIPITATIONS HORAIRE": False,
        "CUMUL DES DUREES D'INSOLATION": False,
        "NOMBRE DE JOURS AVEC TN<=+20°C": False,
        "NOMBRE DE JOURS AVEC FXI>=28 M/S": False,
        "NOMBRE DE JOURS AVEC TN>=+25°C": False,
        "ETP CALCULEE AU POINT DE GRILLE LE PLUS PROCHE": False,
        "VITESSE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "TEMPERATURE A -10 CM HORAIRE": False,
        "HEURE DU TN SOUS ABRI QUOTIDIENNE": False,
        "VISIBILITE HORAIRE": False,
        "NEBULOSITE DE LA 1ERE COUCHE NUAGEUSE": False,
        "TEMPERATURE DE CHAUSSEE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 3 HEURES": False,
        "CODE TEMPS PRESENT HORAIRE": False,
        "OCCURRENCE DE FUMEE QUOTIDIENNE": False,
        "EPAISSEUR DE NEIGE TOTALE HORAIRE": False,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE": False,
        "RAPPORT INSOLATION QUOTIDIEN": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 6 HEURES": False,
        "SOMME DES ETP PENMAN": False,
        "NOMBRE DE JOURS AVEC TN<=+10°C": False,
        "TEMPERATURE A -20 CM HORAIRE": False,
        "NOMBRE DE JOURS AVEC FXI3S>=10M/S": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE": False,
        "PRESSION STATION HORAIRE": False,
        "EPAISSEUR DE NEIGE TOTALE RELEVEE A 0600 FU": False,
        "CUMUL DECADAIRE DES TM>6 AVEC TM ECRETEE A 30 POUR TX": False,
        "MAXIMUM QUOTIDIEN DES EPAISSEURS DE NEIGE TOTALE HORAIRE": False,
        "HUMIDITE RELATIVE MINI MENSUELLE": False,
        "ETAT DE LA MER HORAIRE": False,
        "CODE TEMPS PASSE W2 HORAIRE": False,
        "NEBULOSITE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 100 MM": False,
        "HAUTEUR DE PRECIPITATIONS HORAIRE": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 200 MM": False,
        "OCCURRENCE DE BROUILLARD QUOTIDIENNE": False,
        "AMPLITUDE ENTRE TN ET TX QUOTIDIEN": False,
        "HEURE DU TX SOUS ABRI QUOTIDIENNE": False,
        "DUREE HUMIDITE >= 80% QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC TX>=30°C": False,
        "CUMUL DECADAIRE DES TM>8 AVEC TM ECRETEE A 30 POUR TX": False,
        "NOMBRE DE JOURS AVEC TX>=25°C": False,
        "MOYENNE DES TM": False,
        "ETP PENMAN DECADAIRE": False,
        "NEBUL. DE LA COUCHE NUAG. PRINCIPALE LA PLUS BASSE HORAIRE": False,
        "NOMBRE DE JOURS AVEC RR>=30 MM": False,
        "CUMUL DECADAIRE DES TM>0 AVEC TM NON ECRETEE": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 150 MM": False,
        "CODE SYNOP NUAGES ELEVE HORAIRE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE SUR 3 SECONDES": False,
        "DUREE HUMIDITE<=40% HORAIRE": False,
        "HEURE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "HUMIDITE RELATIVE MOYENNE": False,
        "DUREE D'INSOLATION QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MAXIMALE QUOTIDIENNE": False,
        "MOYENNE DECADAIRE DE LA FORCE DU VENT": False,
        "GEOPOTENTIEL HORAIRE": False,
        "NOMBRE DE JOURS AVEC TX>=35°C": False,
        "NOMBRE DE JOURS AVEC FXI3S>=28M/S": False,
        "MOYENNE DES VITESSES DU VENT A 10M QUOTIDIENNE": False,
        "DUREE HUMIDITE>=80% HORAIRE": False,
        "NOMBRE DE JOURS AVEC TX<=0°C": False,
        "EVAPO-TRANSPIRATION MONTEITH QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC RR>=5 MM": False,
        "MOYENNE DECADAIRE DE LA TENSION DE VAPEUR": False,
        "NEBULOSITE DE LA 4EME COUCHE NUAGEUSE": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE": False,
        "TENSION DE VAPEUR MOYENNE": False,
        "MINIMUM DES TX DU MOIS": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 100 MM": False,
        "BASE DE LA 1ERE COUCHE NUAGEUSE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 8°C": False,
        "DUREE DE GEL QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC RR>=100 MM": False,
        "MAX DES FXY QUOTIDIEN": False,
        "NOMBRE DE JOURS AVEC GRELE": False,
        "DIRECTION DU VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "NOMBRE DE JOURS AVEC RR>=50 MM": False,
        "CUMUL DES DJU SEUIL 18 METHODE METEO": False,
        "RAYONNEMENT DIRECT HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "NOMBRE DE JOURS AVEC TN<=-5°C": False,
        "BASE DE LA 4EME COUCHE NUAGEUSE": False,
        "CUMUL DES HAUTEURS DE PRECIPITATIONS": False,
        "DUREE AVEC VISIBILITE<200 M": False,
        "OCCURRENCE D'ORAGE QUOTIDIENNE": False,
        "ETAT DU SOL AVEC NEIGE HORAIRE": False,
        "CODE TEMPS PASSE W1 HORAIRE": False,
        "DUREE TOTALE D'INSOLATION DECADAIRE": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "HAUTEUR TOTALE DECADAIRE DES PRECIPITATIONS": False,
        "EPAISSEUR MAXIMALE DE NEIGE": False,
        "HEURE DU MAXI D'HUMIDITE QUOTIDIENNE": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN": False,
        "HEURE DU MINI D'HUMIDITE QUOTIDIENNE": False,
        "NEBULOSITE DE LA 3EME COUCHE NUAGEUSE": False,
        "VISIBILITE VERS LA MER": False,
        "CUMUL DU RAYONNEMENT GLOBAL QUOTIDIEN": False,
        "OCCURRENCE DE GRELE QUOTIDIENNE": False,
        "BASE DE LA 3EME COUCHE NUAGEUSE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 0°C": False,
        "OCCURRENCE DE BRUME QUOTIDIENNE": False,
        "MOYENNE DES (TN+TX)/2": False,
        "HUMIDITE RELATIVE MAXI HORAIRE": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 200 MM": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE SOUS ABRI HORAIRE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 50 MM": False,
        "SOMME DES RAYONNEMENTS IR HORAIRE": False,
        "NOMBRE DE JOURS AVEC SIGMA<=20%": False,
        "TEMPERATURE MINIMALE A +10CM QUOTIDIENNE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 200 MM": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 1 HEURE": False,
        "RAYONNEMENT ULTRA VIOLET QUOTIDIEN": False,
        "HUMIDITE RELATIVE MAXI MENSUELLE": False,
        "NOMBRE DE JOURS AVEC TN<=+15°C": False,
        "MAX DES INDICES UV HORAIRE": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "BASE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "NOMBRE DE JOURS AVEC TN<=-10°C": False,
        "NOMBRE DE JOURS AVEC TM>=+24°C": False,
        "DUREE DE GEL HORAIRE": False,
        "VITESSE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "RAYONNEMENT GLOBAL HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "TEMPERATURE MOYENNE SOUS ABRI QUOTIDIENNE": False,
        "DUREE DES PRECIPITATIONS QUOTIDIENNES": False,
        "INDICE UV HORAIRE (COMPRIS ENTRE 0 ET 12)": False,
        "NOMBRE DE JOURS AVEC SIGMA=0%": False,
        "OCCURRENCE DE ROSEE QUOTIDIENNE": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN": False,
        "CUMUL DE PRECIPITATIONS EN 6 MN": False,
        "MOYENNE DES TN DU MOIS": False,
        "PRECIPITATION MAXIMALE EN 24H": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 150 MM": False,
        "NBRE DE JOURS PRESENTS AVEC ORAGE": False,
        "QUANTITE DE PRECIPITATIONS LORS DE L'EPISODE PLUVIEUX": False,
        "NOMBRE DE JOURS AVEC TX>=32°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 10°C": False,
        "HEURE DE L'HUMIDITE RELATIVE MAXIMALE HORAIRE": False,
        "RAYONNEMENT DIRECT QUOTIDIEN": True,
        "VITESSE VENT MAXI INSTANTANE": False,
        "TEMPERATURE MINIMALE SOUS ABRI QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC FXI>=16 M/S": False,
        "TENSION DE VAPEUR HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE": False,
        "TYPE DE LA 3EME COUCHE NUAGEUSE": False,
        "ETAT DU SOL SANS NEIGE HORAIRE": False,
        "TX MAXI DU MOIS": False,
        "NOMBRE DE JOURS AVEC GELEE": False,
        "HAUTEUR DE PRECIPITATIONS QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC TX<=27°C": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 100 MM": False,
        "CUMUL DU RAYONNEMENT DIRECT QUOTIDIEN": False,
        "PRESSION MER HORAIRE": False,
        "DUREE D'INSOLATION HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "TEMPERATURE MAXIMALE SOUS ABRI QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MINI HORAIRE": False,
        "TEMPERATURE MINIMALE A +10CM HORAIRE": False,
        "MINIMUM DE LA PRESSION MER": False,
        "TEMPERATURE DU POINT DE ROSEE HORAIRE": False,
        "TYPE DE LA 2EME COUCHE NUAGEUSE": False,
        "TN MINI DU MOIS": False,
        "PRESSION MER MINIMUM QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC SOL COUVERT DE NEIGE": False,
        "NOMBRE DE JOURS AVEC RR>=1 MM": False,
        "MOYENNE DES TX DU MOIS": False,
        "CUMUL DECADAIRE DES TM>10, TM ECRETEE A 30 POUR TX, 10 POUR TN": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 100 MM": False,
        "MOYENNE DES PRESSIONS MER": False,
        "HUMIDITE RELATIVE HORAIRE": False,
        "NOMBRE DE JOURS AVEC SIGMA>=80%": False,
        "MINIMUM ABSOLU DES PMERM": False,
        "MAXIMUM DES TN DU MOIS": False,
        "HAUTEUR DE NEIGE FRAICHE TOMBEE EN 24H": False,
        "TEMPERATURE A -50 CM HORAIRE": False,
        "NEBULOSITE DE LA 2EME COUCHE NUAGEUSE": False,
        "NOMBRE DE JOURS AVEC FXI>=10 M/S": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 50 MM": False,
        "DUREE HUMIDITE <= 40% QUOTIDIENNE": False,
        "CODE SYNOP NUAGES BAS HORAIRE": False,
        "HEURE DE L'HUMIDITE RELATIVE MINIMALE HORAIRE": False,
        "NOMBRE DE JOURS AVEC RR>=10 MM": False,
        "HUMIDITE RELATIVE MINIMALE QUOTIDIENNE": False,
        "OCCURRENCE DE SOL COUVERT DE NEIGE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 150 MM": False,
        "OCCURRENCE DE NEIGE QUOTIDIENNE": False,
        "DIRECTION DE LA HOULE HORAIRE": False,
        "TEMPERATURE MINI A +50CM QUOTIDIENNE": False,
        "NBRE DE JOURS PRESENT AVEC BROUILLARD": False,
        "NOMBRE DE JOURS AVEC FXI3S>=16M/S": False,
        "OCCURRENCE DE GELEE BLANCHE QUOTIDIENNE": False,
        "OCCURRENCE ECLAIR QUOTIDIENNE": False,
        "HEURE DU TN SOUS ABRI HORAIRE": False,
        "TEMPERATURE MINI A +50CM HORAIRE": False,
        "PRESSION MER MOYENNE QUOTIDIENNE": False,
        "DIRECTION VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "CODE SYNOP NUAGES MOYEN HORAIRE": False,
        "DIRECTION DU VENT MAXI INSTANTANE": False,
        "CUMUL DES DJU SEUIL 18 METHODE CHAUFFAGISTE": False,
        "TEMPERATURE A -100 CM HORAIRE": False,
        "NOMBRE DE JOURS AVEC NEIGE": False,
        "OCCURRENCE DE GRESIL QUOTIDIENNE": False,
        "OCCURRENCE DE VERGLAS": False,
        "TENSION DE VAPEUR MOYENNE QUOTIDIENNE": False,
        "CUMUL DE RAYONNEMENT GLOBAL DECADAIRE": False,
        "TYPE DE LA 1ERE COUCHE NUAGEUSE": False,
        "NOMBRE DE JOURS AVEC TN>=+20°C": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE SUR 3 SECONDES": False,
        "TYPE DE LA 4EME COUCHE NUAGEUSE": False,
        "DUREE HUMECTATION": False,
    }
    return (selection_meteo_renouvelables,)


@app.cell
def _(liste_parametres_actifs, selection_meteo_renouvelables):
    def check_if_parameters_missing(lst1: list[str], lst2: list[str]) -> bool:
        set1, set2 = set(lst1), set(lst2)
        missing_in_1 = set1 - set2
        missing_in_2 = set2 - set1

        if not missing_in_1 and not missing_in_2:
            print("OK!")
            return True

        if missing_in_1:
            print(f"n missing in original {len(missing_in_1)}")
            print(missing_in_1)

        if missing_in_2:
            print(f"n missing in AI generated {len(missing_in_2)}")
            print(missing_in_2)

        return False

    check_if_parameters_missing(
        liste_parametres_actifs,
        list(selection_meteo_renouvelables.keys()),
    )
    return (check_if_parameters_missing,)


@app.cell
def _(mo):
    mo.md(r"""
    ## All active stations (all types) — filter and enrich
    """)
    return


@app.cell
def _(df_with_only_active_stations, pl):
    _df = df_with_only_active_stations.with_columns(
        pl.col("typesPoste")
        .list.eval(
            pl.element().struct.field("dateFin").is_null()
            | (pl.element().struct.field("dateFin") == "")
        )
        .list.sum()
        .alias("n_postes_actifs")
    )

    print(
        "number of stations with more than 1 open 'poste' -- should be 0:",
        _df.filter((pl.col("n_postes_actifs") > 1) | (pl.col("n_postes_actifs") < 1)).shape,
    )
    df_stations_single_poste = _df.filter(pl.col("n_postes_actifs") == 1)
    return (df_stations_single_poste,)


@app.cell
def _(df_stations_single_poste, pl):
    _condition_exclusion = (pl.element().struct.field("dateFin").is_not_null()) & (
        pl.element().struct.field("dateFin") != ""
    )

    df_active_stations_filtered = df_stations_single_poste.with_columns(
        pl.col("parametres")
        .list.eval(pl.element().filter(~_condition_exclusion))
        .alias("parametres_actifs"),
        pl.col("typesPoste")
        .list.eval(pl.element().filter(~_condition_exclusion))
        .alias("type_poste_actif"),
    )

    df_active_stations_filtered
    return (df_active_stations_filtered,)


@app.cell
def _(df_active_stations_filtered, pl):
    liste_parametres_actifs_toutes_stations_ouvertes = (
        (
            df_active_stations_filtered.select(
                pl.col("parametres_actifs").list.eval(pl.element().struct.field("nom"))
            )
            .explode("parametres_actifs")
            .drop_nulls()
            .unique()
        )
        .to_series()
        .sort()
        .to_list()
    )

    liste_parametres_actifs_toutes_stations_ouvertes
    return (liste_parametres_actifs_toutes_stations_ouvertes,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Parameter selection v2 — comprehensive renewable energy parameters

    Refined version of the parameter selection covering solar PV,
    wind energy, and air density variables. Built from the full list
    of active parameters across all open stations.
    """)
    return


@app.cell
def _():
    selection_meteo_renouvelables_v2 = {
        # --- SOLAIRE PHOTOVOLTAÏQUE (Gisement et Rendement) ---
        "RAYONNEMENT GLOBAL HORAIRE": True,
        "RAYONNEMENT GLOBAL HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "RAYONNEMENT DIRECT HORAIRE": True,
        "RAYONNEMENT DIRECT HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "DUREE D'INSOLATION HORAIRE": True,
        "DUREE D'INSOLATION HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "NEBULOSITE TOTALE HORAIRE": True,
        "TEMPERATURE SOUS ABRI HORAIRE": True,
        "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE": True,
        "TEMPERATURE DU POINT DE ROSEE HORAIRE": True,
        # --- ÉOLIEN (Force, Direction et Densité) ---
        "VITESSE DU VENT HORAIRE": True,
        "DIRECTION DU VENT A 10 M HORAIRE": True,
        "MOYENNE DES VITESSES DU VENT A 10M": True,
        "VITESSE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": True,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES": True,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES": True,
        "DIRECTION DU VENT MAXI INSTANTANE SUR 3 SECONDES": True,
        "VITESSE DU VENT A 2 METRES HORAIRE": True,
        "DIRECTION DU VENT A 2 METRES HORAIRE": True,
        "PRESSION STATION HORAIRE": True,
        # --- INDICATEURS DE POTENTIEL (Statistiques) ---
        "NOMBRE DE JOURS AVEC FXY>=8 M/S": True,
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": True,
        "RAYONNEMENT GLOBAL QUOTIDIEN": True,
        "RAYONNEMENT DIRECT QUOTIDIEN": True,
        # --- PARAMÈTRES REJETÉS ---
        "AMPLITUDE ENTRE TN ET TX QUOTIDIEN": False,
        "BASE DE LA 1ERE COUCHE NUAGEUSE": False,
        "BASE DE LA 2EME COUCHE NUAGEUSE": False,
        "BASE DE LA 3EME COUCHE NUAGEUSE": False,
        "BASE DE LA 4EME COUCHE NUAGEUSE": False,
        "BASE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "CODE SYNOP NUAGES BAS HORAIRE": False,
        "CODE SYNOP NUAGES ELEVE HORAIRE": False,
        "CODE SYNOP NUAGES MOYEN HORAIRE": False,
        "CODE TEMPS PASSE W1 HORAIRE": False,
        "CODE TEMPS PASSE W2 HORAIRE": False,
        "CODE TEMPS PRESENT HORAIRE": False,
        "CUMUL DE PRECIPITATIONS EN 6 MN": False,
        "CUMUL DE RAYONNEMENT GLOBAL DECADAIRE": False,
        "CUMUL DECADAIRE DES TM>0 AVEC TM NON ECRETEE": False,
        "CUMUL DECADAIRE DES TM>10, TM ECRETEE A 30 POUR TX, 10 POUR TN": False,
        "CUMUL DECADAIRE DES TM>6 AVEC TM ECRETEE A 30 POUR TX": False,
        "CUMUL DECADAIRE DES TM>8 AVEC TM ECRETEE A 30 POUR TX": False,
        "CUMUL DES DJU SEUIL 18 METHODE CHAUFFAGISTE": False,
        "CUMUL DES DJU SEUIL 18 METHODE METEO": False,
        "CUMUL DES DUREES D'INSOLATION": False,
        "CUMUL DES HAUTEURS DE PRECIPITATIONS": False,
        "CUMUL DU RAYONNEMENT DIRECT QUOTIDIEN": False,
        "CUMUL DU RAYONNEMENT GLOBAL QUOTIDIEN": False,
        "DIRECTION DE LA HOULE HORAIRE": False,
        "DIRECTION DU VENT INSTANTANE MAXI HORAIRE A 2 M": False,
        "DIRECTION DU VENT INSTANTANE MAXI QUOTIDIEN A 2 M": False,
        "DIRECTION DU VENT MAXI INSTANTANE": False,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE": False,
        "DIRECTION DU VENT MOYEN SUR 10 MN MAXIMAL HORAIRE": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "DIRECTION VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "DUREE AVEC VISIBILITE<200 M": False,
        "DUREE D'INSOLATION QUOTIDIENNE": False,
        "DUREE DE GEL HORAIRE": False,
        "DUREE DE GEL QUOTIDIENNE": False,
        "DUREE DES PRECIPITATIONS HORAIRE": False,
        "DUREE DES PRECIPITATIONS QUOTIDIENNES": False,
        "DUREE HUMECTATION": False,
        "DUREE HUMECTATION QUOTIDIENNE": False,
        "DUREE HUMIDITE <= 40% QUOTIDIENNE": False,
        "DUREE HUMIDITE >= 80% QUOTIDIENNE": False,
        "DUREE HUMIDITE<=40% HORAIRE": False,
        "DUREE HUMIDITE>=80% HORAIRE": False,
        "DUREE TOTALE D'INSOLATION DECADAIRE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 100 MM": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 150 MM": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 200 MM": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 50 MM": False,
        "ENFONCEMENT DU TUBE DE NEIGE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 1 HEURE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 3 HEURES": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 6 HEURES": False,
        "EPAISSEUR DE NEIGE TOTALE HORAIRE": False,
        "EPAISSEUR DE NEIGE TOTALE RELEVEE A 0600 FU": False,
        "EPAISSEUR MAXIMALE DE NEIGE": False,
        "ETAT DE LA COUCHE SUPERFICIELLE DE NEIGE": False,
        "ETAT DE LA MER HORAIRE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DU SOL AVEC NEIGE HORAIRE": False,
        "ETAT DU SOL SANS NEIGE HORAIRE": False,
        "ETP CALCULEE AU POINT DE GRILLE LE PLUS PROCHE": False,
        "ETP PENMAN DECADAIRE": False,
        "EVAPO-TRANSPIRATION MONTEITH QUOTIDIENNE": False,
        "GEOPOTENTIEL HORAIRE": False,
        "HAUTEUR DE NEIGE FRAICHE TOMBEE EN 24H": False,
        "HAUTEUR DE PRECIPITATIONS HORAIRE": False,
        "HAUTEUR DE PRECIPITATIONS QUOTIDIENNE": False,
        "HAUTEUR ESTIMEE DES PRECIPS MENSUELLES": False,
        "HAUTEUR TOTALE DECADAIRE DES PRECIPITATIONS": False,
        "HEURE DE L'HUMIDITE RELATIVE MAXIMALE HORAIRE": False,
        "HEURE DE L'HUMIDITE RELATIVE MINIMALE HORAIRE": False,
        "HEURE DU MAXI D'HUMIDITE QUOTIDIENNE": False,
        "HEURE DU MINI D'HUMIDITE QUOTIDIENNE": False,
        "HEURE DU TN SOUS ABRI HORAIRE": False,
        "HEURE DU TN SOUS ABRI QUOTIDIENNE": False,
        "HEURE DU TX SOUS ABRI HORAIRE": False,
        "HEURE DU TX SOUS ABRI QUOTIDIENNE": False,
        "HEURE DU VENT MAX INSTANTANE A 2 M HORAIRE": False,
        "HEURE DU VENT MAX INSTANTANE A 2 M QUOTIDIENNE": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE SUR 3 SECONDES": False,
        "HEURE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "HEURE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "HUMIDITE RELATIVE HORAIRE": False,
        "HUMIDITE RELATIVE MAXI HORAIRE": False,
        "HUMIDITE RELATIVE MAXI MENSUELLE": False,
        "HUMIDITE RELATIVE MAXIMALE QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MINI HORAIRE": False,
        "HUMIDITE RELATIVE MINI MENSUELLE": False,
        "HUMIDITE RELATIVE MINIMALE QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MOYENNE": False,
        "INDICE UV HORAIRE (COMPRIS ENTRE 0 ET 12)": False,
        "MAX DES FXY QUOTIDIEN": False,
        "MAX DES INDICES UV HORAIRE": False,
        "MAXIMUM DES TN DU MOIS": False,
        "MAXIMUM QUOTIDIEN DES EPAISSEURS DE NEIGE TOTALE HORAIRE": False,
        "MINIMUM ABSOLU DES PMERM": False,
        "MINIMUM DE LA PRESSION MER": False,
        "MINIMUM DES TX DU MOIS": False,
        "MOYENNE DECADAIRE DE LA FORCE DU VENT": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MAXI": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MINI": False,
        "MOYENNE DECADAIRE DE LA TENSION DE VAPEUR": False,
        "MOYENNE DES (TN+TX)/2": False,
        "MOYENNE DES PRESSIONS MER": False,
        "MOYENNE DES TM": False,
        "MOYENNE DES TN DU MOIS": False,
        "MOYENNE DES TX DU MOIS": False,
        "MOYENNE DES VITESSES DU VENT A 10M QUOTIDIENNE": False,
        "MOYENNE DES VITESSES DU VENT A 2 METRES QUOTIDIENNE": False,
        "MOYENNE MENSUELLE ESTIMEE DES TN DU MOIS": False,
        "MOYENNE MENSUELLE ESTIMEE DES TX DU MOIS": False,
        "NBRE DE JOURS PRESENT AVEC BROUILLARD": False,
        "NBRE DE JOURS PRESENTS AVEC ORAGE": False,
        "NEBUL. DE LA COUCHE NUAG. PRINCIPALE LA PLUS BASSE HORAIRE": False,
        "NEBULOSITE DE LA 1ERE COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 2EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 3EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 4EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "NOMBRE DE JOURS AVEC FXI3S>=10M/S": False,
        "NOMBRE DE JOURS AVEC FXI3S>=16M/S": False,
        "NOMBRE DE JOURS AVEC FXI3S>=28M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=10 M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=16 M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=28 M/S": False,
        "NOMBRE DE JOURS AVEC FXY>=15 M/S": False,
        "NOMBRE DE JOURS AVEC GELEE": False,
        "NOMBRE DE JOURS AVEC GRELE": False,
        "NOMBRE DE JOURS AVEC NEIGE": False,
        "NOMBRE DE JOURS AVEC RR>=1 MM": False,
        "NOMBRE DE JOURS AVEC RR>=10 MM": False,
        "NOMBRE DE JOURS AVEC RR>=100 MM": False,
        "NOMBRE DE JOURS AVEC RR>=30 MM": False,
        "NOMBRE DE JOURS AVEC RR>=5 MM": False,
        "NOMBRE DE JOURS AVEC RR>=50 MM": False,
        "NOMBRE DE JOURS AVEC SIGMA<=20%": False,
        "NOMBRE DE JOURS AVEC SIGMA=0%": False,
        "NOMBRE DE JOURS AVEC SIGMA>=80%": False,
        "NOMBRE DE JOURS AVEC SOL COUVERT DE NEIGE": False,
        "NOMBRE DE JOURS AVEC TM>=+24°C": False,
        "NOMBRE DE JOURS AVEC TN<=+10°C": False,
        "NOMBRE DE JOURS AVEC TN<=+15°C": False,
        "NOMBRE DE JOURS AVEC TN<=+20°C": False,
        "NOMBRE DE JOURS AVEC TN<=-10°C": False,
        "NOMBRE DE JOURS AVEC TN<=-5°C": False,
        "NOMBRE DE JOURS AVEC TN>=+20°C": False,
        "NOMBRE DE JOURS AVEC TN>=+25°C": False,
        "NOMBRE DE JOURS AVEC TX<=0°C": False,
        "NOMBRE DE JOURS AVEC TX<=20°C": False,
        "NOMBRE DE JOURS AVEC TX<=27°C": False,
        "NOMBRE DE JOURS AVEC TX>=25°C": False,
        "NOMBRE DE JOURS AVEC TX>=30°C": False,
        "NOMBRE DE JOURS AVEC TX>=32°C": False,
        "NOMBRE DE JOURS AVEC TX>=35°C": False,
        "OCCURRENCE D'ORAGE QUOTIDIENNE": False,
        "OCCURRENCE DE BROUILLARD QUOTIDIENNE": False,
        "OCCURRENCE DE BRUME QUOTIDIENNE": False,
        "OCCURRENCE DE FUMEE QUOTIDIENNE": False,
        "OCCURRENCE DE GELEE BLANCHE QUOTIDIENNE": False,
        "OCCURRENCE DE GRELE QUOTIDIENNE": False,
        "OCCURRENCE DE GRESIL QUOTIDIENNE": False,
        "OCCURRENCE DE NEIGE QUOTIDIENNE": False,
        "OCCURRENCE DE ROSEE QUOTIDIENNE": False,
        "OCCURRENCE DE SOL COUVERT DE NEIGE": False,
        "OCCURRENCE DE VERGLAS": False,
        "OCCURRENCE ECLAIR QUOTIDIENNE": False,
        "PRECIPITATION MAXIMALE EN 24H": False,
        "PRESSION MER HORAIRE": False,
        "PRESSION MER MINIMUM QUOTIDIENNE": False,
        "PRESSION MER MOYENNE QUOTIDIENNE": False,
        "QUANTITE DE PRECIPITATIONS LORS DE L'EPISODE PLUVIEUX": False,
        "QUANTITE PRECIP BRUTE": False,
        "RAPPORT INSOLATION QUOTIDIEN": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "RAYONNEMENT ULTRA VIOLET QUOTIDIEN": False,
        "SOMME DES ETP PENMAN": False,
        "SOMME DES RAYONNEMENTS IR HORAIRE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 0°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 10°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 6°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 8°C": False,
        "TEMPERATURE A -10 CM HORAIRE": False,
        "TEMPERATURE A -100 CM HORAIRE": False,
        "TEMPERATURE A -20 CM HORAIRE": False,
        "TEMPERATURE A -50 CM HORAIRE": False,
        "TEMPERATURE DE CHAUSSEE": False,
        "TEMPERATURE DE SURFACE DE LA NEIGE": False,
        "TEMPERATURE MAXIMALE SOUS ABRI QUOTIDIENNE": False,
        "TEMPERATURE MINI A +50CM HORAIRE": False,
        "TEMPERATURE MINI A +50CM QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE A +10CM HORAIRE": False,
        "TEMPERATURE MINIMALE A +10CM QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE SOUS ABRI HORAIRE": False,
        "TEMPERATURE MINIMALE SOUS ABRI QUOTIDIENNE": False,
        "TEMPERATURE MOYENNE SOUS ABRI QUOTIDIENNE": False,
        "TENSION DE VAPEUR HORAIRE": False,
        "TENSION DE VAPEUR MOYENNE": False,
        "TENSION DE VAPEUR MOYENNE QUOTIDIENNE": False,
        "TN MINI DU MOIS": False,
        "TX MAXI DU MOIS": False,
        "TYPE DE LA 1ERE COUCHE NUAGEUSE": False,
        "TYPE DE LA 2EME COUCHE NUAGEUSE": False,
        "TYPE DE LA 3EME COUCHE NUAGEUSE": False,
        "TYPE DE LA 4EME COUCHE NUAGEUSE": False,
        "VISIBILITE HORAIRE": False,
        "VISIBILITE VERS LA MER": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE A 2M": False,
        "VITESSE DU VENT INSTANTANE MAXI QUOTIDIEN A 2 M": False,
        "VITESSE VENT MAXI INSTANTANE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE SUR 3 SECONDES": False,
        "VITESSE VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "VITESSE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
    }

    return (selection_meteo_renouvelables_v2,)


@app.cell
def _(
    check_if_parameters_missing,
    liste_parametres_actifs_toutes_stations_ouvertes,
    selection_meteo_renouvelables_v2,
):
    check_if_parameters_missing(
        liste_parametres_actifs_toutes_stations_ouvertes,
        list(selection_meteo_renouvelables_v2.keys()),
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Filter stations by relevant parameters
    """)
    return


@app.cell
def _(
    df_active_stations_filtered,
    pl,
    selection_meteo_renouvelables_v2,
):
    parameters_to_keep = [
        param for param, selected in selection_meteo_renouvelables_v2.items() if selected
    ]

    df_with_relevant_params = df_active_stations_filtered.with_columns(
        pl.col("parametres_actifs")
        .list.eval(pl.element().filter(pl.element().struct.field("nom").is_in(parameters_to_keep)))
        .alias("parametres_actifs_pertinents")
    )

    df_with_relevant_params
    return (df_with_relevant_params,)


@app.cell
def _(df_with_relevant_params, pl):
    # Stations that lost all parameters after filtering (should be investigated)
    print(
        "stations with 0 relevant params:",
        df_with_relevant_params.filter(
            pl.col("parametres_actifs_pertinents").list.len() == 0
        ).shape,
    )
    return


@app.cell
def _(df_with_relevant_params, pl):
    # Data quality checks
    print(
        "duplicated on id: ",
        df_with_relevant_params.filter(pl.col("id").is_duplicated()).shape,
    )

    print(
        "count(type_post_actif) != 1",
        df_with_relevant_params.filter(pl.col("type_poste_actif").list.len() != 1).shape,
    )

    print(
        "null_count",
        df_with_relevant_params.null_count().to_dicts()[0],
    )
    return


@app.cell
def _(df_with_relevant_params, pl):
    # Station distribution by poste type
    stats_par_type = (
        df_with_relevant_params.explode("type_poste_actif")
        .drop_nulls("type_poste_actif")
        .group_by(pl.col("type_poste_actif").struct.field("type"))
        .agg(pl.len().alias("nb_stations"))
        .sort("type")
    )

    print(stats_par_type)
    # Safe to keep only type [0:2] per MF documentation
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Rename and select final columns
    """)
    return


@app.cell
def _(df_with_relevant_params, pl):
    column_mapping = {
        "id": "id",
        "nom": "nom",
        "lieuDit": "lieu_dit",
        "bassin": "bassin",
        "dateDebut": "date_debut",
        "dateFin": "date_fin",
        "positions": "positions",
        "typesPoste": "types_poste",
        "n_postes_actifs": "n_postes",
        "type_poste_actif": "type_poste_actif",
        "producteurs": "producteurs",
        "parametres": "parametres",
        "parametres_actifs": "parametres_actifs",
        "parametres_actifs_pertinents": ("parametres_actifs_pertinents"),
    }

    df_stations_clean = df_with_relevant_params.select(
        [pl.col(old).alias(new) for old, new in column_mapping.items()]
    )
    return (df_stations_clean,)


@app.cell
def _(df_stations_clean, df_with_relevant_params):
    print(
        len(df_with_relevant_params.columns),
        df_with_relevant_params.columns,
    )
    print(
        len(df_stations_clean.columns),
        df_stations_clean.columns,
    )
    return


@app.cell
def _(df_stations_clean):
    df_stations_clean
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Cross-reference with observations API
    """)
    return


@app.cell
def _(bronze_path, pl):
    liste_stations_path = bronze_path / "liste-stations-obs-metropole-om.csv"
    df_from_api = pl.read_csv(
        liste_stations_path,
        separator=";",
        has_header=True,
        schema_overrides={"Id_station": pl.String},
    )

    print(df_from_api.columns)
    print(df_from_api.shape)
    return (df_from_api,)


@app.cell
def _(df_stations_clean, pl):
    df_stations_type_0_to_2 = df_stations_clean.filter(
        pl.col("type_poste_actif")
        .list.eval(pl.element().struct.field("type").is_in([0, 1, 2]))
        .list.sum()
        > 0
    )
    return (df_stations_type_0_to_2,)


@app.cell
def _(df_from_api, df_stations_type_0_to_2):
    # Stations in API but not in our reference
    stations_pas_dans_le_referentiel = df_from_api.join(
        df_stations_type_0_to_2,
        left_on="Id_station",
        right_on="id",
        how="anti",
    )

    print(
        "stations_pas_dans_le_referentiel:",
        stations_pas_dans_le_referentiel.shape,
    )
    stations_pas_dans_le_referentiel
    return


@app.cell
def _(df_from_api, df_stations_type_0_to_2):
    # Stations in our reference but not in API
    stations_pas_dans_api = df_stations_type_0_to_2.join(
        df_from_api,
        left_on="id",
        right_on="Id_station",
        how="anti",
    )

    print(
        "stations_pas_dans_api:",
        stations_pas_dans_api.shape,
    )
    stations_pas_dans_api
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Type 0 only — final subset for climatology pipeline
    """)
    return


@app.cell
def _(df_stations_clean, pl):
    # Type 0 = synoptic stations: highest data quality, used by climatology API
    df_stations_type_0 = df_stations_clean.filter(
        pl.col("type_poste_actif").list.eval(pl.element().struct.field("type") == 0).list.sum() > 0
    )
    print(f"type 0 stations: {df_stations_type_0.shape[0]}")
    df_stations_type_0
    return


if __name__ == "__main__":
    app.run()
