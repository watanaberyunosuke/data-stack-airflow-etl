WITH product_lookup AS (
    SELECT DISTINCT ON (product_name, city)
        product_id,
        product_name,
        city
    FROM {{ ref('raw_products') }}
    ORDER BY
        product_name,
        city,
        product_id
),

internal_events AS (
    SELECT
        'core_platform' AS event_source,
        0 AS reseller_id,
        t.transaction_id,
        t.customer_id,
        t.product_id,
        p.product_name,
        t.quantity,
        t.amount AS total_amount,
        t.order_method_id,
        om.order_method_name,
        p.city,
        t.transaction_date,
        t.load_timestamp,
        NULL::VARCHAR AS imported_file
    FROM {{ ref('raw_transactions') }} AS t
    LEFT JOIN {{ ref('raw_products') }} AS p
        ON t.product_id = p.product_id
    LEFT JOIN {{ ref('raw_order_methods') }} AS om
        ON t.order_method_id = om.order_method_id
),

csv_events AS (
    SELECT
        'partner_csv' AS event_source,
        c.reseller_id,
        c.transaction_id,
        c.customer_id,
        p.product_id,
        c.product_name,
        c.quantity,
        c.total_amount,
        om.order_method_id,
        c.order_method_name,
        c.city,
        c.transaction_date,
        c.load_timestamp,
        c.imported_file
    FROM {{ ref('raw_resellerscsv') }} AS c
    LEFT JOIN product_lookup AS p
        ON c.product_name = p.product_name
        AND c.city = p.city
    LEFT JOIN {{ ref('raw_order_methods') }} AS om
        ON c.order_method_name = om.order_method_name
),

xml_events AS (
    SELECT
        'partner_xml' AS event_source,
        x.reseller_id,
        x.transaction_id,
        x.customer_id,
        p.product_id,
        x.product_name,
        x.quantity,
        x.total_amount,
        x.order_method_id,
        om.order_method_name,
        x.city,
        x.transaction_date,
        x.load_timestamp,
        x.imported_file
    FROM {{ ref('raw_resellersxml') }} AS x
    LEFT JOIN product_lookup AS p
        ON x.product_name = p.product_name
        AND x.city = p.city
    LEFT JOIN {{ ref('raw_order_methods') }} AS om
        ON x.order_method_id = om.order_method_id
),

all_events AS (
    SELECT * FROM internal_events
    UNION ALL
    SELECT * FROM csv_events
    UNION ALL
    SELECT * FROM xml_events
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'event_source',
        'reseller_id',
        'transaction_id',
        'transaction_date',
        'customer_id',
        'product_name',
        'quantity',
        'total_amount',
        'order_method_id',
        'city',
        'imported_file'
    ]) }} AS event_key,
    event_source,
    reseller_id,
    transaction_id,
    customer_id,
    product_id,
    product_name,
    quantity,
    total_amount,
    order_method_id,
    order_method_name,
    city,
    transaction_date,
    imported_file,
    load_timestamp,
    CASE
        WHEN event_source = 'core_platform' THEN FALSE
        ELSE TRUE
    END AS is_partner_feed
FROM all_events
