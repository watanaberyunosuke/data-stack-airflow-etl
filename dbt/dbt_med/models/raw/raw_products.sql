WITH raw_products AS (
    SELECT * FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,
    product_name,
    price,
    city,
    now() AS load_timestamp
FROM raw_products
