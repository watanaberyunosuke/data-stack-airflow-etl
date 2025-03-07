WITH raw_order_methods AS (
    SELECT * FROM {{ source('raw', 'order_methods') }}
)

SELECT
    order_method_id,
    order_method_name

FROM raw_order_methods
