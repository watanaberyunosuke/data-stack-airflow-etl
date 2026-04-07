WITH staging_order_methods AS (
    SELECT
        order_method_id,
        order_method_name
    FROM {{ ref('raw_order_methods') }}
)

SELECT
    order_method_id AS order_method_key,
    order_method_id,
    order_method_name,
    CASE
        WHEN order_method_name IN ('Walk-in Clinic', 'Hospital Discharge')
            THEN 'In-person'
        WHEN order_method_name = 'Patient Portal'
            THEN 'Digital Self-service'
        ELSE 'Clinician-mediated'
    END AS access_channel_group
FROM staging_order_methods
