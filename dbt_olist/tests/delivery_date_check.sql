-- ensuring delivered orders have delivery dates

{{ config(severity='warn', store_failures = true) }}

SELECT
    order_id,
    order_status,
    order_delivered_customer_date
FROM {{ ref('STG_orders') }}
-- Filter for orders marked as 'delivered'
WHERE order_status = 'delivered'
-- Identify the problem: the final delivery date is NULL
  AND order_delivered_customer_date IS NULL

LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures