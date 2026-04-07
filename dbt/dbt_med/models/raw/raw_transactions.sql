WITH raw_transactions AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
)

SELECT
    transaction_id,
    customer_id,
    product_id,
    amount,
    quantity,
    order_method_id,
    transaction_date,
    load_timestamp
FROM raw_transactions
ORDER BY transaction_date
