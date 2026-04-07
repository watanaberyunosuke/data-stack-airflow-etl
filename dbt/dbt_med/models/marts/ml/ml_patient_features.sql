WITH anchor AS (
    SELECT MAX(transaction_date) AS max_transaction_date
    FROM {{ ref('fct_dispensing_events') }}
),

ordered_events AS (
    SELECT
        patient_key,
        transaction_date,
        LAG(transaction_date) OVER (
            PARTITION BY patient_key
            ORDER BY transaction_date
        ) AS previous_transaction_date
    FROM {{ ref('fct_dispensing_events') }}
),

patient_gaps AS (
    SELECT
        patient_key,
        ROUND(AVG(
            transaction_date - previous_transaction_date
        )::NUMERIC, 2) AS avg_days_between_dispenses
    FROM ordered_events
    WHERE previous_transaction_date IS NOT NULL
    GROUP BY patient_key
),

patient_stats AS (
    SELECT
        patient_key,
        MIN(transaction_date) AS first_dispense_date,
        MAX(transaction_date) AS last_dispense_date,
        COUNT(*) AS lifetime_dispenses,
        SUM(quantity) AS lifetime_quantity,
        ROUND(SUM(total_amount), 2) AS lifetime_amount,
        COUNT(DISTINCT medication_key) AS distinct_medications,
        COUNT(DISTINCT partner_key) AS distinct_partners,
        COUNT(DISTINCT order_method_key) AS distinct_access_channels,
        ROUND(AVG(total_amount), 2) AS avg_event_value,
        ROUND(
            AVG(CASE WHEN is_partner_feed THEN 1.0 ELSE 0.0 END),
            4
        ) AS partner_feed_ratio
    FROM {{ ref('fct_dispensing_events') }}
    GROUP BY patient_key
)

SELECT
    p.patient_key,
    p.patient_id,
    p.first_name,
    p.last_name,
    p.email_domain,
    s.first_dispense_date,
    s.last_dispense_date,
    a.max_transaction_date - s.last_dispense_date AS days_since_last_dispense,
    s.lifetime_dispenses,
    s.lifetime_quantity,
    s.lifetime_amount,
    s.distinct_medications,
    s.distinct_partners,
    s.distinct_access_channels,
    s.avg_event_value,
    COALESCE(g.avg_days_between_dispenses, 0) AS avg_days_between_dispenses,
    s.partner_feed_ratio,
    CASE
        WHEN s.lifetime_dispenses >= 4
            AND COALESCE(g.avg_days_between_dispenses, 0) >= 45
            THEN TRUE
        ELSE FALSE
    END AS adherence_risk_label,
    ROUND(
        LEAST(
            1.0,
            (
                (a.max_transaction_date - s.last_dispense_date)::NUMERIC / 120.0
                + s.partner_feed_ratio
                + (s.distinct_access_channels::NUMERIC / 10.0)
            )
        ),
        4
    ) AS outreach_risk_score
FROM {{ ref('dim_patients') }} AS p
LEFT JOIN patient_stats AS s
    ON p.patient_key = s.patient_key
LEFT JOIN patient_gaps AS g
    ON p.patient_key = g.patient_key
CROSS JOIN anchor AS a
WHERE s.patient_key IS NOT NULL
