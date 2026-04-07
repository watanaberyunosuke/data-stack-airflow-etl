SELECT
    reseller_key AS partner_key,
    reseller_id AS partner_id,
    reseller_name AS partner_name,
    partner_type,
    commission_pct
FROM {{ ref('staging_resellers') }}
