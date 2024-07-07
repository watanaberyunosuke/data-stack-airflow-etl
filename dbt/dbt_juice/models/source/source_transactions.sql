WITH raw_transactions AS (
    SELECT * FROM {{ source('extracts', 'transactions') }}
)

SELECT
    customer_id,
    product_id,
    amount,
    qty,
    channel_id,
    bought_date,
    transaction_id,
    now() AS loaded_timestamp
FROM raw_transactions
