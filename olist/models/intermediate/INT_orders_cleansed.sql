-- OBJECTIVE: This model ensures that only logically consistent orders are passed to the next layers by
-- removing orders that are marked as 'delivered' but have no payment record.

WITH orders AS (
    SELECT * FROM {{ ref('STG_orders') }}
),

payments AS (
    SELECT DISTINCT order_id 
    FROM {{ ref('STG_order_payments') }}
),

-- Identify the specific rows that violate the core integrity rule (Delivered but Unpaid)
inconsistent_orders AS (
    SELECT 
        o.order_id
    FROM orders as o
    LEFT JOIN payments p 
        ON o.order_id = p.order_id
    WHERE 
        o.order_status IN ('delivered', 'shipped', 'invoiced') 
        AND p.order_id IS NULL
)

-- Final Step - Return all good orders
SELECT 
    o.*
FROM orders as o
LEFT JOIN inconsistent_orders as i
    ON o.order_id = i.order_id
WHERE 
    i.order_id IS NULL