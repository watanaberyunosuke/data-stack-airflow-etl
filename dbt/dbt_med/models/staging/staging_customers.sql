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
        customer_first_name AS first_name,
        customer_last_name AS last_name,
        NULL::VARCHAR AS email
    FROM
        {{ ref('raw_resellerscsv') }}
),

customers_xml AS (
    SELECT
        customer_id,
        customer_first_name AS first_name,
        customer_last_name AS last_name,
        NULL::VARCHAR AS email
    FROM
        {{ ref('raw_resellersxml') }}
),

customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM
        customers_csv
    UNION
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM
        customers_xml
    UNION
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM
        customer_main
),

resolved_customers AS (
    SELECT
        customer_id,
        MAX(first_name) FILTER (
            WHERE first_name IS NOT NULL
        ) AS first_name,
        MAX(last_name) FILTER (
            WHERE last_name IS NOT NULL
        ) AS last_name,
        MAX(email) FILTER (
            WHERE email IS NOT NULL
        ) AS email
    FROM
        customers
    GROUP BY
        customer_id
)


SELECT
{{ dbt_utils.generate_surrogate_key([
    'customer_id'
]) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email
FROM resolved_customers
