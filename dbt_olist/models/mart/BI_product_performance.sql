-- product_performance.sql
-- Description: Marked layer for product performance

SELECT
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold,
    SUM(oi.price) AS total_revenue,
    round(AVG(r.review_score),2) AS avg_review_score,
    round(AVG(
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATE_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)
        END
    ),2) AS avg_delivery_days
FROM {{ ref('DIM_products') }} AS p
LEFT JOIN {{ ref('FACT_order_items') }} AS oi
    ON p.product_id = oi.product_id
LEFT JOIN {{ ref('FACT_orders') }} AS o
    ON oi.order_id = o.order_id AND o.order_status = 'delivered'
LEFT JOIN {{ ref('DIM_order_reviews') }} AS r
    ON oi.order_id = r.order_id
GROUP BY
    p.product_id,
    p.product_category_name
ORDER BY total_revenue DESC
