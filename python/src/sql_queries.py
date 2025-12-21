# SQL Queries for Olist Analytics

### RAW DATA (STAGING MODELS)

GET_CUSTOMERS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.customers`
"""
GET_GEOLOCATION = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.geolocation`
"""
GET_ORDER_ITEMS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.order_items`
"""
GET_ORDER_PAYMENTS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.order_payments`
"""
GET_ORDER_REVIEWS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.order_reviews`
"""
GET_ORDERS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.orders`
"""
GET_PRODUCTS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.products`
"""
GET_SELLERS = """
SELECT * FROM `olist-ecommerce-1234321.rawdata.sellers`
"""

### PROCESSED DATA (MART MODELS)

# Stand-alone business intelligence tables

GET_BI_CUSTOMER_COHORTS = """
SELECT * FROM `olist-ecommerce-1234321.mart.BI_customer_cohorts`
"""
GET_BI_CUSTOMER_RFM = """
SELECT * FROM `olist-ecommerce-1234321.mart.BI_customer_rfm`
"""
GET_BI_DELIVERY_PERFORMANCE = """
SELECT * FROM `olist-ecommerce-1234321.mart.BI_delivery_performance`
"""
GET_BI_PRODUCT_PERFORMANCE = """
SELECT * FROM `olist-ecommerce-1234321.mart.BI_product_performance`
"""
GET_BI_SELLER_PERFORMANCE = """
SELECT * FROM `olist-ecommerce-1234321.mart.BI_seller_performance`
"""

# Star schema fact and dimension tables

GET_DIM_CUSTOMERS = """ 
SELECT * FROM `olist-ecommerce-1234321.mart.DIM_customers`
"""
GET_DIM_ORDER_REVIEWS = """
SELECT * FROM `olist-ecommerce-1234321.mart.DIM_order_reviews`
"""
GET_DIM_PRODUCTS = """
SELECT * FROM `olist-ecommerce-1234321.mart.DIM_products`
"""
GET_DIM_SELLERS = """
SELECT * FROM `olist-ecommerce-1234321.mart.DIM_sellers`
"""
GET_FACT_ORDERS = """
SELECT * FROM `olist-ecommerce-1234321.mart.FACT_orders`
"""
GET_FACT_ORDER_ITEMS = """
SELECT * FROM `olist-ecommerce-1234321.mart.FACT_order_items`
"""

### ADDITIONAL ANALYTICS QUERIES

# Completed Orders - Daily Sales and Orders
## note that the accepted values in the Where caluse depends on the business definition of 'completed'
GET_completed_daily_orders = """
SELECT 
    date(order_purchase_timestamp) AS order_purchase_date,
    count(DISTINCT order_id) AS total_daily_orders,
    sum(payment_value) AS total_daily_revenue
FROM `olist-ecommerce-1234321.mart.FACT_orders`
WHERE order_status IN ('delivered','shipped') 
GROUP BY order_purchase_date
"""

# Canceled Orders - Daily Sales and Orders
GET_canceled_daily_orders = """
SELECT 
    date(order_purchase_timestamp) AS order_purchase_date,
    count(DISTINCT order_id) AS total_daily_orders,
    sum(payment_value) AS total_daily_revenue
FROM `olist-ecommerce-1234321.mart.FACT_orders`
WHERE order_status = 'canceled'
GROUP BY order_purchase_date
"""

# delivery performance 

GET_delivery_performance = """
SELECT 
    order_id, seller_id, actual_delivery_days, delay_vs_estimate, fulfillment_days, on_time_flag 
FROM `olist-ecommerce-1234321.mart.BI_delivery_performance`
WHERE fulfillment_days IS NOT NULL  -- excluding a few cases with Null values
"""

# delivery performance with purchase_date 

GET_delivery_duration_time_series = """
SELECT 
  DATE(o.order_purchase_timestamp) AS order_purchase_date,
  round(AVG(dp.actual_delivery_days),2) AS days_to_delivery

FROM `olist-ecommerce-1234321.mart.FACT_orders` AS o
JOIN `olist-ecommerce-1234321.mart.BI_delivery_performance` AS dp 
  ON o.order_id = dp.order_id
GROUP BY date(o.order_purchase_timestamp)
"""

# product category performance

GET_product_category_performance = """
SELECT
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COALESCE(COUNT(oi.order_item_id), 0) AS total_items_sold,
    COALESCE(SUM(oi.price + oi.freight_value), 0) AS total_revenue
FROM `olist-ecommerce-1234321.mart.DIM_products` AS p
LEFT JOIN `olist-ecommerce-1234321.mart.FACT_order_items` AS oi
    ON p.product_id = oi.product_id 
LEFT JOIN `olist-ecommerce-1234321.mart.FACT_orders` AS o    
    ON oi.order_id = o.order_id
   AND o.order_status = 'delivered'
GROUP BY p.product_category_name
"""


# Region (province) performance

GET_region_performance ="""
WITH customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        province,
        latitude,
        longitude
    FROM `olist-ecommerce-1234321.mart.DIM_customers`
    WHERE province IS NOT NULL
)

SELECT  
    c.province,
    AVG(c.latitude) AS latitude,
    AVG(c.longitude) AS longitude,
    COUNT(DISTINCT c.customer_unique_id) AS total_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.payment_value) AS total_spending
FROM customers c
JOIN `olist-ecommerce-1234321.mart.FACT_orders` o
    ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.province
"""

# Overal Business Metrics

GET_overal_business_metrics = """
WITH delivered_orders AS (
    SELECT *
    FROM `olist-ecommerce-1234321.mart.FACT_orders`
    WHERE order_status = 'delivered'
),
order_summary AS (
    SELECT 
        COUNT(*) AS total_orders,
        SUM(payment_value) AS total_revenue,
        AVG(payment_value) AS avg_order_value  -- Average order value including freight
    FROM delivered_orders
),
items_summary AS (
    SELECT COUNT(*) AS total_items
    FROM `olist-ecommerce-1234321.mart.FACT_order_items` oi
    JOIN delivered_orders o ON oi.order_id = o.order_id
),
customer_summary AS (
    SELECT COUNT(DISTINCT c.customer_unique_id) AS total_customers
    FROM `olist-ecommerce-1234321.mart.DIM_customers` c
    JOIN delivered_orders o ON c.customer_id = o.customer_id
),
seller_summary AS (
    SELECT COUNT(DISTINCT oi.seller_id) AS total_sellers
    FROM `olist-ecommerce-1234321.mart.FACT_order_items` oi
    JOIN delivered_orders o ON oi.order_id = o.order_id
)
SELECT 
    cs.total_customers AS total_customers,
    ss.total_sellers AS total_sellers,
    os.total_orders AS total_orders, 
    isum.total_items AS total_items_ordered,
    os.total_revenue AS total_revenue,
    os.avg_order_value AS avg_order_value,
    CASE WHEN os.total_orders > 0 THEN isum.total_items * 1.0 / os.total_orders ELSE 0 END AS avg_basket_size
FROM customer_summary cs
CROSS JOIN seller_summary ss
CROSS JOIN order_summary os
CROSS JOIN items_summary isum

"""

GET_monthly_time_series = """
WITH orders AS (
    SELECT
        order_id,
        customer_id,
        DATE_TRUNC(order_purchase_timestamp, MONTH) AS month,
        payment_value
    FROM `olist-ecommerce-1234321.mart.FACT_orders`
    WHERE order_status = 'delivered'
),
order_items_quantity AS (
    SELECT
        order_id,
        COUNT(*) AS total_items_ordered
    FROM `olist-ecommerce-1234321.mart.FACT_order_items`
    GROUP BY order_id
),
sellers_per_month AS (
    SELECT
        o.month,
        COUNT(DISTINCT oi.seller_id) AS total_sellers
    FROM orders o
    JOIN `olist-ecommerce-1234321.mart.FACT_order_items` oi ON o.order_id = oi.order_id
    GROUP BY o.month
)

SELECT
    o.month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    s.total_sellers,
    SUM(q.total_items_ordered) AS total_items_ordered,
    SUM(o.payment_value) AS total_revenue,
    ROUND(SUM(o.payment_value) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value,
    ROUND(SUM(q.total_items_ordered) / COUNT(DISTINCT o.order_id), 2) AS avg_basket_size
FROM orders o
JOIN order_items_quantity q ON o.order_id = q.order_id
JOIN sellers_per_month s ON o.month = s.month
GROUP BY o.month, s.total_sellers
ORDER BY o.month
"""

