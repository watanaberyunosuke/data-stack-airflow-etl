SELECT
    f.transaction_date,
    f.city,
    d.partner_name,
    o.order_method_name,
    o.access_channel_group,
    COUNT(*) AS dispense_events,
    COUNT(DISTINCT f.patient_key) AS unique_patients,
    COUNT(DISTINCT f.medication_key) AS unique_medications,
    SUM(f.quantity) AS total_quantity,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_event_value,
    SUM(CASE WHEN f.is_partner_feed THEN 1 ELSE 0 END) AS partner_feed_events
FROM {{ ref('fct_dispensing_events') }} AS f
LEFT JOIN {{ ref('dim_partners') }} AS d
    ON f.partner_key = d.partner_key
LEFT JOIN {{ ref('dim_order_methods') }} AS o
    ON f.order_method_key = o.order_method_key
GROUP BY
    f.transaction_date,
    f.city,
    d.partner_name,
    o.order_method_name,
    o.access_channel_group
