-- payment_test_4.sql
-- TEST OBJECTIVE: Identify failed orders that still have a payment record, suggesting a pending refund liability.
-- SEVERITY: Warning
-- FOCUS: Intersection of failed orders and existing payments.
-- FAILURE MODE: High volume suggests poor inventory/fulfillment or slow refund processing.

{{ config(severity='warn') }}

SELECT 
    o.order_id,
    o.order_status,
    p.payment_value 
FROM {{ ref('STG_orders') }} as o
-- INNER JOIN isolates ONLY the orders that are both (1) failed AND (2) have a payment.
INNER JOIN {{ ref('INT_order_payments_agg') }} as p 
    ON o.order_id = p.order_id
WHERE 
    -- The criteria: Order failed but payment exists
    o.order_status IN ('canceled', 'unavailable')