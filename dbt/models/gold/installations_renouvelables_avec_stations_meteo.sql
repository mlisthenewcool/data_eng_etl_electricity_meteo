{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold model: each active renewable installation (solar/wind) matched to its
    nearest weather station capable of measuring the relevant parameters.

    Uses PostGIS KNN operator (<->) with GiST indexes for fast nearest-neighbour
    lookup, then ST_Distance for the exact WGS84 distance in metres.
*/

with installations_renouvelables as (
    select
        inst.id_peps,
        inst.nom_installation,
        inst.code_iris,
        inst.code_departement,
        inst.code_region,
        inst.type_energie,
        inst.puis_max_installee,
        iris.centroid_lat as installation_lat,
        iris.centroid_lon as installation_lon,
        ST_SetSRID(
            ST_MakePoint(iris.centroid_lon, iris.centroid_lat), 4326
        )::geography as geog
    from {{ ref('stg_dim_installations') }} as inst
    inner join {{ ref('stg_dim_contours_iris') }} as iris
        on inst.code_iris = iris.code_iris
    where
        inst.est_renouvelable
        and inst.est_actif
        and inst.type_energie in ('solaire', 'eolien')
)

select
    ir.id_peps,
    ir.nom_installation,
    ir.code_iris,
    ir.code_departement,
    ir.code_region,
    ir.type_energie,
    ir.puis_max_installee,
    ir.installation_lat,
    ir.installation_lon,
    nearest.station_id,
    nearest.station_nom,
    nearest.station_lat,
    nearest.station_lon,
    round(
        ST_Distance(ir.geog, nearest.geog)::numeric / 1000, 2
    ) as distance_km
from installations_renouvelables as ir
cross join lateral (
    select
        st.station_id,
        st.station_nom,
        st.station_lat,
        st.station_lon,
        st.geog
    from {{ ref('stg_dim_stations_meteo') }} as st
    where
        case ir.type_energie
            when 'solaire' then st.mesure_solaire
            when 'eolien'  then st.mesure_eolien
        end
    order by ir.geog <-> st.geog
    limit 1
) as nearest
