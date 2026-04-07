WITH patient_partner_edges AS (
    SELECT
        'patient' AS source_node_type,
        patient_key::TEXT AS source_node_id,
        'partner' AS target_node_type,
        partner_key::TEXT AS target_node_id,
        'patient_partner_dispense' AS edge_type,
        COUNT(*)::NUMERIC AS edge_weight,
        ROUND(SUM(total_amount), 2) AS financial_weight,
        MAX(transaction_date) AS last_event_date
    FROM {{ ref('fct_dispensing_events') }}
    WHERE patient_key IS NOT NULL
        AND partner_key IS NOT NULL
    GROUP BY patient_key, partner_key
),

patient_medication_edges AS (
    SELECT
        'patient' AS source_node_type,
        patient_key::TEXT AS source_node_id,
        'medication' AS target_node_type,
        medication_key::TEXT AS target_node_id,
        'patient_medication_dispense' AS edge_type,
        COUNT(*)::NUMERIC AS edge_weight,
        ROUND(SUM(total_amount), 2) AS financial_weight,
        MAX(transaction_date) AS last_event_date
    FROM {{ ref('fct_dispensing_events') }}
    WHERE patient_key IS NOT NULL
        AND medication_key IS NOT NULL
    GROUP BY patient_key, medication_key
),

partner_medication_edges AS (
    SELECT
        'partner' AS source_node_type,
        partner_key::TEXT AS source_node_id,
        'medication' AS target_node_type,
        medication_key::TEXT AS target_node_id,
        'partner_medication_mix' AS edge_type,
        COUNT(*)::NUMERIC AS edge_weight,
        ROUND(SUM(total_amount), 2) AS financial_weight,
        MAX(transaction_date) AS last_event_date
    FROM {{ ref('fct_dispensing_events') }}
    WHERE partner_key IS NOT NULL
        AND medication_key IS NOT NULL
    GROUP BY partner_key, medication_key
)

SELECT * FROM patient_partner_edges
UNION ALL
SELECT * FROM patient_medication_edges
UNION ALL
SELECT * FROM partner_medication_edges
