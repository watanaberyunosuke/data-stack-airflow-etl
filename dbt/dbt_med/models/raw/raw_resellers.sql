WITH raw_resellers AS (
    SELECT * FROM {{ source('raw', 'resellers') }}
)

SELECT
    reseller_id,
    reseller_name,
    commission_pct
FROM raw_resellers
