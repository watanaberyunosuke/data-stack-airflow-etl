WITH products AS (
    SELECT *
    FROM {{ ref('raw_products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    product_name,
    REGEXP_REPLACE(product_name, '\s+[0-9].*$', '') AS medication_name,
    CASE
        WHEN product_name ILIKE '%metformin%' THEN 'Diabetes'
        WHEN product_name ILIKE '%amlodipine%'
            OR product_name ILIKE '%losartan%'
            OR product_name ILIKE '%lisinopril%'
            OR product_name ILIKE '%metoprolol%'
            THEN 'Cardiovascular'
        WHEN product_name ILIKE '%atorvastatin%'
            OR product_name ILIKE '%clopidogrel%'
            THEN 'Cardiometabolic'
        WHEN product_name ILIKE '%duloxetine%'
            OR product_name ILIKE '%sertraline%'
            OR product_name ILIKE '%gabapentin%'
            THEN 'Mental Health and Pain'
        WHEN product_name ILIKE '%salbutamol%' THEN 'Respiratory'
        WHEN product_name ILIKE '%amoxicillin%' THEN 'Infection'
        ELSE 'General Care'
    END AS therapeutic_area,
    city,
    price,
    load_timestamp
FROM products
