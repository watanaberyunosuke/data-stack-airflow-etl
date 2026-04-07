WITH partner_master AS (
    SELECT
        reseller_id,
        reseller_name,
        commission_pct
    FROM {{ ref('raw_resellers') }}

    UNION ALL

    SELECT
        0 AS reseller_id,
        'Primary Care Platform' AS reseller_name,
        0::NUMERIC AS commission_pct
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['reseller_id']) }} AS reseller_key,
    reseller_id,
    reseller_name,
    commission_pct,
    CASE
        WHEN reseller_name ILIKE '%Clinic%' THEN 'Clinic'
        WHEN reseller_name ILIKE '%Hospital%' THEN 'Hospital'
        WHEN reseller_name ILIKE '%Telehealth%' THEN 'Telehealth'
        WHEN reseller_name ILIKE '%Network%' THEN 'Network'
        ELSE 'Internal Platform'
    END AS partner_type
FROM partner_master
