-- checking the payment check test 1 failure cases...
SELECT 
    o.order_id,
    p.payment_installments,
    COUNT(*) AS total_mismatches,
    ROUND(p.payment_value - ((o.total_price + o.total_freight_value)), 2) AS diff,
    COUNT(*) AS orders_with_diff
FROM {{ ref('INT_order_payments_agg') }} p
JOIN {{ ref('INT_order_items_agg') }} o
  ON p.order_id = o.order_id
JOIN {{ ref('STG_orders') }} s
  ON p.order_id = s.order_id
WHERE  s.order_status IN ('delivered', 'shipped', 'invoiced')
GROUP BY order_id, p.payment_installments, diff
ORDER BY ABS(diff) DESC
