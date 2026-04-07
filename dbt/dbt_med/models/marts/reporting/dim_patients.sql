SELECT
    customer_key AS patient_key,
    customer_id AS patient_id,
    first_name,
    last_name,
    email,
    SPLIT_PART(email, '@', 2) AS email_domain
FROM {{ ref('staging_customers') }}
