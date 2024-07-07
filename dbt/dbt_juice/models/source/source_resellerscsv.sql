WITH raw_resellercsv AS (
    SELECT * FROM {{ source('extracts', 'resellercsv') }}
)

SELECT
    transaction_id,
    product_name,
    quantity,
    total_amount,
    order_method,
    customer_first_name,
    customer_last_name,
    city,
    transaction_date
FROM raw_resellercsv
