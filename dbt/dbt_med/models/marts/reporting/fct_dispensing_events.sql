WITH events AS (
    SELECT *
    FROM {{ ref('staging_partner_events') }}
)

SELECT
    e.event_key,
    c.customer_key AS patient_key,
    p.product_key AS medication_key,
    r.reseller_key AS partner_key,
    o.order_method_key,
    e.event_source,
    e.transaction_id,
    e.transaction_date,
    e.city,
    e.quantity,
    e.total_amount,
    e.is_partner_feed,
    e.imported_file,
    e.load_timestamp
FROM events AS e
LEFT JOIN {{ ref('staging_customers') }} AS c
    ON e.customer_id = c.customer_id
LEFT JOIN {{ ref('staging_products') }} AS p
    ON e.product_id = p.product_id
LEFT JOIN {{ ref('staging_resellers') }} AS r
    ON e.reseller_id = r.reseller_id
LEFT JOIN {{ ref('staging_order_method') }} AS o
    ON e.order_method_id = o.order_method_id
