WITH raw_transactions AS (
    SELECT * FROM {{ source('extracts', 'transactions') }}
)

SELECT
    customer_id,
    product_id,
    amount,
    quantity,
    order_method_id,
    transaction_date,
    transaction_id,
    now() AS loaded_timestamp
FROM raw_transactions
ORDER BY transaction_date
