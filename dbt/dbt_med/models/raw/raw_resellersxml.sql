WITH raw_resellersxml AS (
    SELECT *
    FROM
        {{ source(
            'preprocessed',
            'resellersxmlextracted'
        ) }}
)

SELECT
    reseller_id,
    transaction_id,
    product_name,
    quantity,
    total_amount,
    order_method_id,
    customer_id,
    customer_first_name,
    customer_last_name,
    city,
    transaction_date,
    imported_file,
    load_timestamp
FROM raw_resellersxml
