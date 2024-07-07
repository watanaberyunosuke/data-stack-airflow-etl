WITH raw_order_methods AS (
    SELECT * FROM {{ source('extracts', 'order_methods') }}
)

SELECT
    order_method_id,
    order_method_name,
    loaded_timestamp

FROM raw_order_methods
