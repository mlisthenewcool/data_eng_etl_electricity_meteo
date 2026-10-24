WITH k AS (
    SELECT *,
        codeEICResourceObject IS NULL AS est_agregation,
        'AGR_' || coalesce('IRIS_' || codeIRIS, 'COM_' || codeINSEECommune, 'DEP_' || codeDepartement,
                           'REG_' || codeRegion, 'FR') || '_' || codeFiliere AS _base_key
    FROM {{ ref('bronze_odre_installations') }}
),
keyed AS (
    SELECT *,
        CASE WHEN est_agregation
             THEN _base_key || '_' || CAST(sum(est_agregation::INT) OVER (PARTITION BY _base_key ORDER BY _source_row ROWS UNBOUNDED PRECEDING) AS VARCHAR)
             ELSE codeEICResourceObject END AS code_eic_resource_object
    FROM k
)
SELECT
    code_eic_resource_object,
    codeIRIS AS code_iris, codeRegion AS code_region, codeFiliere AS code_filiere,
    puisMaxInstallee AS puis_max_installee,
    codeFiliere IN ('SOLAI', 'EOLIE') AS est_renouvelable,
    CASE codeFiliere WHEN 'SOLAI' THEN 'solaire' WHEN 'EOLIE' THEN 'eolien' ELSE 'autre' END AS type_energie,
    dateDeraccordement IS NULL AS est_actif
FROM keyed
QUALIFY row_number() OVER (PARTITION BY code_eic_resource_object ORDER BY _source_row DESC) = 1
