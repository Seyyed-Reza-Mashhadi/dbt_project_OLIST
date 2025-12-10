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

# total revenue vs. time (monthly), percent change ?
# average monthly revenue, AOV, ABS 



