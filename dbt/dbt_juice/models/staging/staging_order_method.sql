WITH staging_order_methods AS (
    SELECT
        order_method_id,
        order_method_name
    FROM {{ ref('extracts_order_methods') }}
)

SELECT
    order_method_id AS order_method_key,
    order_method_id,
    order_method_name
FROM staging_order_methods
