-- a model for joining customer, orders and payments

WITH cust AS (
    SELECT 
        c.customer_id,
        c.customer_unique_id,
        oc.order_id,
        oc.order_status,
        oc.order_purchase_timestamp,
        opagg.payment_value
    FROM {{ ref('STG_customers') }} AS c
    LEFT JOIN {{ ref('INT_orders_cleansed') }} AS oc 
        ON oc.customer_id = c.customer_id
    LEFT JOIN {{ ref('INT_order_payments_agg') }} AS opagg 
        ON opagg.order_id = oc.order_id
    WHERE oc.order_status IN ('delivered', 'shipped', 'invoiced')
      AND oc.order_id IS NOT NULL
)

SELECT 
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    payment_value
FROM cust
