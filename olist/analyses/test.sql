-- Checking for duplicates
-- SELECT COUNT(DISTINCT order_id) FROM {{ ref('STG_order_items') }}
-- SELECT COUNT(DISTINCT order_id) FROM {{ ref('INT_order_items_agg') }}
-- SELECT COUNT(DISTINCT order_id) FROM {{ ref('STG_orders') }} 
-- SELECT COUNT(DISTINCT order_id) FROM {{ ref('INT_order_payments_agg') }} 
-- SELECT COUNT(DISTINCT order_id) FROM {{ ref('STG_order_payments') }} 

-- SELECT count( * ) AS counters from {{ ref('INT_geolocation') }} 
