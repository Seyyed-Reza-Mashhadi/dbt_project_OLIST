SELECT
    order_id,
    ROUND(AVG(payment_sequential)) AS payment_sequential,
    STRING_AGG(DISTINCT payment_type, ', ') AS payment_type, -- payment types into a single, comma-separated string
    ROUND(AVG(payment_installments)) AS payment_installments,
    SUM(payment_value) AS payment_value
FROM {{ ref('STG_order_payments') }}
GROUP BY order_id
limit 100 