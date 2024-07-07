WITH raw_resellers AS (
    SELECT * FROM {{ source('extracts', 'resellers') }}
)

SELECT
    reseller_id,
    reseller_name,
    loaded_timestamp
FROM raw_resellers
