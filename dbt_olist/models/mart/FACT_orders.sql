-- FACT_orders.sql
SELECT 
    ---- columns from INT_orders_cleansed
    so.order_id,
    so.customer_id,
    so.order_status,
    so.order_purchase_timestamp,
    so.order_approved_at,
    so.order_delivered_carrier_date,
    so.order_delivered_customer_date,
    so.order_estimated_delivery_date,  
    ----- columns from INT_order_payments_agg
    iaop.payment_sequential,
    iaop.payment_type, 
    iaop.payment_installments,
    iaop.payment_value
    
FROM {{ ref('INT_orders_cleansed') }} AS so
LEFT JOIN {{ ref('INT_order_payments_agg') }} AS iaop
    ON iaop.order_id = so.order_id