SELECT
    order_method_key,
    order_method_id,
    order_method_name,
    access_channel_group
FROM {{ ref('staging_order_method') }}
