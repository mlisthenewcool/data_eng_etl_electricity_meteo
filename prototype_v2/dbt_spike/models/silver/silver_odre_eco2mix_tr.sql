{{ config(materialized='incremental', unique_key=['code_insee_region', 'date_heure'], incremental_strategy='merge') }}
SELECT
    code_insee_region,
    timezone('UTC', date_heure)::TIMESTAMP AS date_heure,
    consommation,
    TRY_CAST(pompage AS BIGINT) AS pompage
FROM {{ ref('bronze_odre_eco2mix_tr') }}
{% if is_incremental() %}
WHERE timezone('UTC', date_heure)::TIMESTAMP >= (SELECT max(date_heure) - INTERVAL 1 DAY FROM {{ this }})
{% endif %}
