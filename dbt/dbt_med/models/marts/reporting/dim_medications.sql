SELECT
    product_key AS medication_key,
    product_id,
    medication_name,
    product_name,
    therapeutic_area,
    city,
    price
FROM {{ ref('staging_products') }}
