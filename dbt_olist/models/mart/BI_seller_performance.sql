-- seller_performance.sql
-- Description: Marked layer for seller performance

SELECT
    oi.seller_id,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold,
    SUM(oi.price) AS total_revenue,
    AVG(
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATE_DIFF(
                DATE(o.order_delivered_customer_date), 
                DATE(o.order_purchase_timestamp), 
                DAY
            )
        END
    ) AS avg_delivery_days,
    AVG(r.review_score) AS avg_review_score
FROM {{ ref('FACT_order_items') }} AS oi
LEFT JOIN {{ ref('FACT_orders') }} AS o
    ON oi.order_id = o.order_id AND o.order_status = 'delivered'
LEFT JOIN {{ ref('DIM_order_reviews') }} AS r
    ON oi.order_id = r.order_id
GROUP BY
    oi.seller_id
ORDER BY total_revenue DESC