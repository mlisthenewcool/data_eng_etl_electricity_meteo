WITH ir AS (
    SELECT i.code_eic_resource_object, i.code_region, i.type_energie, r.centroid_lat AS lat, r.centroid_lon AS lon
    FROM {{ ref('silver_odre_installations') }} i
    JOIN {{ ref('silver_ign_contours_iris') }} r ON i.code_iris = r.code_iris
    WHERE i.est_renouvelable AND i.est_actif
),
best AS (
    SELECT ir.code_eic_resource_object,
        arg_min({'id': s.id, 'lat': s.latitude, 'lon': s.longitude},
                (ST_Distance_Sphere(ST_Point(ir.lon, ir.lat), ST_Point(s.longitude, s.latitude)), s.id)) AS b
    FROM ir JOIN {{ ref('silver_meteo_france_stations') }} s
      ON (ir.type_energie = 'solaire' AND s.mesure_solaire) OR (ir.type_energie = 'eolien' AND s.mesure_eolien)
    GROUP BY ir.code_eic_resource_object
)
SELECT ir.*, b.id AS station_id,
    round(ST_Distance_Spheroid(ST_Point(ir.lon, ir.lat), ST_Point(b.lon, b.lat)) / 1000, 2) AS distance_km
FROM ir JOIN best USING (code_eic_resource_object)
