SELECT code_region, type_energie, count(*) AS installations, avg(distance_km) AS distance_moy_km
FROM {{ ref('gold_installations_stations') }}
GROUP BY ALL
