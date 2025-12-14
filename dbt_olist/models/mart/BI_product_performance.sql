-- product_performance.sql
-- Grain: 1 row per product_id

WITH delivered_orders AS (
    -- 1 row per delivered order
    SELECT
        order_id,
        order_purchase_timestamp,
        order_delivered_customer_date
    FROM {{ ref('FACT_orders') }}
    WHERE order_status = 'delivered'
),

order_items AS (
    -- 1 row per order item (already correct grain)
    SELECT
        order_id,
        product_id,
        order_item_id,
        price,
        freight_value
    FROM {{ ref('FACT_order_items') }}
),

order_reviews AS (
    -- 1 row per order (pre-aggregated → NO duplication)
    SELECT
        order_id,
        AVG(review_score) AS avg_review_score
    FROM {{ ref('DIM_order_reviews') }}
    GROUP BY order_id
)

SELECT
    p.product_id,
    p.product_category_name,

    -- Orders & sales
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold,
    SUM(oi.price + oi.freight_value) AS total_revenue,

    -- Customer experience
    ROUND(AVG(orv.avg_review_score), 2) AS avg_review_score,

    -- Logistics
    ROUND(
        AVG(
            DATE_DIFF(
                d.order_delivered_customer_date,
                d.order_purchase_timestamp,
                DAY
            )
        ),
        2
    ) AS avg_delivery_days

FROM {{ ref('DIM_products') }} AS p

LEFT JOIN order_items AS oi
    ON p.product_id = oi.product_id

LEFT JOIN delivered_orders AS d
    ON oi.order_id = d.order_id

LEFT JOIN order_reviews AS orv
    ON d.order_id = orv.order_id

GROUP BY
    p.product_id,
    p.product_category_name

ORDER BY total_revenue DESC