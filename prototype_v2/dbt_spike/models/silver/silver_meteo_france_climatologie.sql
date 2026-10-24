SELECT
    NUM_POSTE AS id_station,
    strptime(AAAAMMJJHH || ' +0000', '%Y%m%d%H %z') AS date_heure,
    T AS temperature,
    U::SMALLINT AS humidite,
    RR1 AS precipitations
FROM {{ ref('bronze_meteo_france_climatologie') }}
