-- payment_test_3.sql
-- TEST OBJECTIVE: Identify completed orders that are missing line item details.
-- FOCUS: Orders in STG_orders that are 'delivered', 'shipped', or 'invoiced'.
-- FAILURE MODE: Inability to track inventory, or product performance.

{{ config(severity='warn', store_failures = true) }}

SELECT 
    o.order_id,
    o.order_status 
FROM {{ ref('STG_orders') }} as o
LEFT JOIN {{ ref('INT_order_items_agg') }} as i 
    ON o.order_id = i.order_id
WHERE 
    -- 1. Filter the Universe: Only check orders that must have items recorded
    o.order_status IN ('delivered', 'shipped', 'invoiced') 
    -- 2. Failure Condition: The item record (i) is NULL, indicating it's missing
    AND i.order_id IS NULL

LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures