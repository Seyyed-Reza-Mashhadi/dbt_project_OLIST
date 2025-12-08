-- delivery_performance.sql
-- Description: Order-level delivery performance

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.seller_id,
    o.customer_id,
    
    -- Actual delivery time in days
    DATE_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY) AS actual_delivery_days,
    
    -- Seller fulfillment time in days
    DATE_DIFF(o.order_delivered_carrier_date, o.order_purchase_timestamp, DAY) AS fulfillment_days,
    
    -- Delay compared to promised delivery date
    DATE_DIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date, DAY) AS delay_vs_estimate,
    
    -- Boolean flag: TRUE if delivered on or before promised date, else FALSE
    CASE 
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS on_time_flag

FROM {{ ref('FACT_order_items') }} AS oi
LEFT JOIN {{ ref('FACT_orders') }} AS o
    ON oi.order_id = o.order_id
    AND o.order_status = 'delivered'
    
-- ADDED FILTER: Exclude any record where the final delivery date is NULL.
-- This is necessary even if order_status = 'delivered' due to data quality issues.
WHERE o.order_delivered_customer_date IS NOT NULL 

ORDER BY oi.seller_id, oi.order_id