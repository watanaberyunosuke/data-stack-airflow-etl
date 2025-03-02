WITH raw_resellerscsv AS (
    SELECT *
    FROM
        {{ source(
            'raw',
            'resellerscsv'
        ) }}
)

SELECT
    reseller_id,
    transaction_id,
    product_name,
    quantity,
    total_amount,
    order_method,
    customer_id,
    customer_first_name,
    customer_last_name,
    city,
    transaction_date,
    imported_file,
    now() AS load_timestamp
FROM
    raw_resellerscsv
