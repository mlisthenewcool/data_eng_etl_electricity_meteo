{{
    config(
        materialized='view',
        schema='silver'
    )
}}

select
    id_peps,
    nom_installation,
    code_iris,
    code_departement,
    code_region,
    type_energie,
    est_renouvelable,
    est_actif,
    puis_max_installee
from {{ source('silver', 'dim_installations') }}
