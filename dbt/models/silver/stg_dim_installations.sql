SELECT
    code_eic_resource_object,
    nom_installation,
    code_iris,
    code_departement,
    code_region,
    type_energie,
    est_renouvelable,
    est_actif,
    puis_max_installee
FROM {{ source('silver', 'dim_installations') }}
