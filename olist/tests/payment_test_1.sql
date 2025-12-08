-- payment_test_1.sql
-- Test: Aggregated payment must equal (total price + total freight) 
-- for orders that are delivered, shipped, or invoiced.

{{ config(severity='warn', store_failures = true) }}

SELECT 
    p.order_id,
    p.payment_value,
    (o.total_price + o.total_freight_value) AS expected_payment
FROM {{ ref('INT_order_payments_agg') }} as p
INNER JOIN {{ ref('INT_order_items_agg') }} as o 
    ON p.order_id = o.order_id
INNER JOIN {{ ref('STG_orders') }} as s
    ON p.order_id = s.order_id
WHERE 
    s.order_status IN ('delivered', 'shipped', 'invoiced')
    -- Check for a difference greater than a small tolerance (e.g., 10 cent)
    AND ABS(p.payment_value - (o.total_price + o.total_freight_value)) > 0.10

LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures