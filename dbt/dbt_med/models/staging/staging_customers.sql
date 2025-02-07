
WITH customer_main AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM
        {{ ref('raw_customers') }}
),

customers_csv AS (
    SELECT
        customer_id,
        SPLIT_PART(SPLIT_PART(imported_file, '_', 3), '.', 1)::INT AS reseller_id,
        transaction_id
    FROM
        {{ ref('raw_resellerscsv') }}
),

customers_xml AS (
    SELECT
        customer_id,
        reseller_id,
        transaction_id
    FROM
        {{ source(
            'preprocessed',
            'resellersxmlextracted'
        ) }}
),

customers AS (
    SELECT
        reseller_id,
        transaction_id,
        customer_id
    FROM
        customers_csv
    UNION
    SELECT
        reseller_id,
        transaction_id,
        customer_id
    FROM
        customers_xml
    UNION
    SELECT
        0 AS reseller_id,
        customer_id
    FROM
        customer_main
)


SELECT

{{ dbt_utils.generate_surrogate_key([
    'c.reseller_id',
    'customer_id'
]) }} AS customer_key, 
c.*
FROM customers c
LEFT JOIN raw.customers ec ON c.customer_id = ec.customer_id