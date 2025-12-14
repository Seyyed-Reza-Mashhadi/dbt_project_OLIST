WITH seller_orders AS (
    SELECT
        oi.seller_id,
        oi.order_id,
        DATE_DIFF(
            DATE(o.order_delivered_customer_date),
            DATE(o.order_purchase_timestamp),
            DAY
        ) AS delivery_days,
        r.review_score
    FROM {{ ref('FACT_order_items') }} AS oi
    JOIN {{ ref('FACT_orders') }} AS o
        ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('DIM_order_reviews') }} AS r
        ON oi.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        oi.seller_id,
        oi.order_id,
        delivery_days,
        r.review_score
),

seller_items AS (
    SELECT
        oi.seller_id,
        oi.order_id,
        COUNT(oi.order_item_id) AS total_items_sold,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM {{ ref('FACT_order_items') }} AS oi
    JOIN {{ ref('FACT_orders') }} AS o
        ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        oi.seller_id,
        oi.order_id
)

SELECT
    si.seller_id,
    COUNT(DISTINCT so.order_id) AS total_orders,
    SUM(si.total_items_sold) AS total_items_sold,
    SUM(si.total_revenue) AS total_revenue,
    AVG(so.delivery_days) AS avg_delivery_days,
    AVG(DISTINCT so.review_score) AS avg_review_score
FROM seller_items AS si
JOIN seller_orders AS so
    ON si.seller_id = so.seller_id
   AND si.order_id = so.order_id
GROUP BY
    si.seller_id
ORDER BY total_revenue DESC
