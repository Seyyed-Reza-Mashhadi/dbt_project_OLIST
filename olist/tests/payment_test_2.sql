-- payment_test_2.sql
-- TEST OBJECTIVE: Identify completed orders that are missing any financial record.
-- FOCUS: Orders in STG_orders that are 'delivered', 'shipped', or 'invoiced'.
-- FAILURE MODE: Revenue is missing or untraceable for fulfilled goods.

{{ config(severity='warn', store_failures = true) }}

SELECT 
    o.order_id,
    o.order_status 
FROM {{ ref('STG_orders') }} as o
LEFT JOIN {{ ref('INT_order_payments_agg') }} as p 
    ON o.order_id = p.order_id
WHERE 
    -- 1. Filter the Universe: Only check orders that must have been paid
    o.order_status IN ('delivered', 'shipped', 'invoiced') 
    -- 2. Failure Condition: The payment record (p) is NULL, indicating it's missing
    AND p.order_id IS NULL

LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures