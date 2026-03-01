INSERT INTO {schema}.{table} (
    id_station, date_heure,
    rayonnement_global, duree_insolation, nebulosite,
    vitesse_vent, direction_vent, rafale_max,
    temperature, temperature_max, temperature_min, point_de_rosee,
    humidite, precipitations,
    pression_station, pression_mer,
    inserted_at
)
SELECT
    id_station, date_heure,
    rayonnement_global, duree_insolation, nebulosite,
    vitesse_vent, direction_vent, rafale_max,
    temperature, temperature_max, temperature_min, point_de_rosee,
    humidite, precipitations,
    pression_station, pression_mer,
    NOW()
FROM {staging}
ON CONFLICT (id_station, date_heure) DO UPDATE SET
    rayonnement_global  = EXCLUDED.rayonnement_global,
    duree_insolation    = EXCLUDED.duree_insolation,
    nebulosite          = EXCLUDED.nebulosite,
    vitesse_vent        = EXCLUDED.vitesse_vent,
    direction_vent      = EXCLUDED.direction_vent,
    rafale_max          = EXCLUDED.rafale_max,
    temperature         = EXCLUDED.temperature,
    temperature_max     = EXCLUDED.temperature_max,
    temperature_min     = EXCLUDED.temperature_min,
    point_de_rosee      = EXCLUDED.point_de_rosee,
    humidite            = EXCLUDED.humidite,
    precipitations      = EXCLUDED.precipitations,
    pression_station    = EXCLUDED.pression_station,
    pression_mer        = EXCLUDED.pression_mer,
    updated_at          = NOW()
WHERE (
    {schema}.{table}.temperature,
    {schema}.{table}.vitesse_vent,
    {schema}.{table}.rayonnement_global
) IS DISTINCT FROM (
    EXCLUDED.temperature,
    EXCLUDED.vitesse_vent,
    EXCLUDED.rayonnement_global
);
