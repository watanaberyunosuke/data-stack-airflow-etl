WITH raw_customers AS (
    SELECT * FROM {{ source('extracts', 'customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email

FROM raw_customers
