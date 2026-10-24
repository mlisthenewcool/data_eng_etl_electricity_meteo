WITH s AS (
    SELECT *,
        list_filter(positions, lambda x: x.dateFin IS NULL OR x.dateFin = '') AS pos,
        list_transform(list_filter(parametres, lambda x: x.dateFin IS NULL OR x.dateFin = ''), lambda x: x.nom) AS noms
    FROM {{ ref('bronze_meteo_france_stations') }}
    WHERE dateFin IS NULL OR dateFin = ''
)
SELECT
    id, nom,
    pos[1].latitude AS latitude,
    pos[1].longitude AS longitude,
    len(list_filter(noms, lambda n: list_contains(['VITESSE DU VENT HORAIRE', 'PRESSION STATION HORAIRE'], n))) > 0 AS mesure_eolien,
    len(list_filter(noms, lambda n: list_contains(['RAYONNEMENT GLOBAL HORAIRE', 'TEMPERATURE SOUS ABRI HORAIRE'], n))) > 0 AS mesure_solaire,
    len(noms)::UINTEGER AS nb_parametres
FROM s
WHERE pos[1].latitude IS NOT NULL
