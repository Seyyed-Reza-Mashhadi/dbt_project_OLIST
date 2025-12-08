SELECT 
    order_id,
    round(SUM(price),2) AS total_price,
    round(SUM(freight_value),2) AS total_freight_value,
FROM {{ ref('STG_order_items') }}
GROUP BY order_id