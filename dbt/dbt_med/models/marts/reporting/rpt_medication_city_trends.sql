SELECT
    DATE_TRUNC('month', f.transaction_date)::DATE AS activity_month,
    f.city,
    m.medication_name,
    m.therapeutic_area,
    COUNT(*) AS dispense_events,
    COUNT(DISTINCT f.patient_key) AS unique_patients,
    SUM(f.quantity) AS total_quantity,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM {{ ref('fct_dispensing_events') }} AS f
LEFT JOIN {{ ref('dim_medications') }} AS m
    ON f.medication_key = m.medication_key
GROUP BY
    DATE_TRUNC('month', f.transaction_date)::DATE,
    f.city,
    m.medication_name,
    m.therapeutic_area
