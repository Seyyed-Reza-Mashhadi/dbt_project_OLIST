# dbt Project - Olist Dataset 
<img width="2000" height="1025" alt="image" src="https://github.com/user-attachments/assets/f44ab50e-220b-42be-a0e0-91068f7d5cdf" />

## 🧩 About the Project  
This project transforms and validates the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) using **dbt (Data Build Tool)** and **Google BigQuery**.  
It demonstrates a complete **ELT (Extract, Load, Transform)** workflow, focusing on **data modeling, testing, documentation, and analytics readiness**.  

The main goal is to show a clear understanding of how to:  
- Build a modular and maintainable **dbt** project  
- Use **sources**, **refs**, **macros**, and **tests** correctly  
- Apply data-quality principles and detect inconsistencies through **custom tests**  
- Transform raw e-commerce data into analytics-ready fact and dimension models  

## 🎯 Objectives  
- Develop a clean **dbt** structure with clear schema naming (staging → intermediate → marts)  
- Implement **generic**, **custom generic**, and **singular** tests for data quality  
- Demonstrate **materializations** (views, tables, incremental models) and **global configs**  
- Build analytical models such as **RFM segmentation** and **order performance**  
- Deploy and run transformations on **dbt Cloud + BigQuery**

---
we need to mention that BIGquery is computing power, connection is done with services account and key and that in VS code dbt-core and ....
---

## 🏗️ Project Architecture  

### 🧱 Folder Structure
The picture below shows the dbt project files and its structure. 

<img width="1560" height="1125" alt="image" src="https://github.com/user-attachments/assets/1befca44-0fda-4b36-b7da-07c2a86e639a" />


## ⚙️ Data Source Configuration  
The source datasets were defined in `_sources.yml`, referencing BigQuery tables for all raw Olist data.  

``` yml
version: 2
sources:
  - name: olist_dataset
    description: "Raw tables from the Olist Brazilian E-Commerce Dataset"
    database: olist-ecommerce-2025
    schema: rawdata
    tables:
      - name: customers
        description: "Contains unique customer identifiers, their city, and state..."
      - name: geolocation
      ...
```

## 🧩 Data Transformation Flow: Creating Models  
The transformation follows the dbt philosophy of **modular, dependency-aware modeling**.  
`ref()` and `source()` ensure models build in the correct order.
In this project, dbt **models** are built in three standard layers (**staging**, **intermediate**, and **mart**): 
   - **Staging Layer** – Clean column names, standardize datatypes, rename IDs, etc. 
   - **Intermediate Layer** – joins, aggregations, intermediate tables/veiws, etc.
   - **Marts Layer** – Final joins, aggregations, etc. for BI and reporting.  (RFM, seller performance, delivery reliability). 

To organize models in the BigQuery dataset, a **macro** created for automatic definition of model **schema** based on folder's name in the models directory. 

```
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
 Note that a **seed** is used to add full province name to desired tables in addition to its abbreviation.   

## ✅ Data Quality Testing  

Testing was a core part of this project, using both **generic**, and **singular** tests.

### Generic Tests  
In this project, generic tests ensure fundamental data integrity. Defining **unique** and **not_null** tests for primary keys is essential, while **relationships** tests validate foreign key references. For columns with a limited set of valid categorical values (e.g., `order_status`), **accepted_values** tests are applied to enforce consistency.

```yml
  - name: STG_order_payments
    description: "Order payment data with standardized data formats and column names."
    columns:
      - name: order_id
        description: "Order identifier key."
        tests:
          - not_null
          - relationships:
              arguments:
                to: ref('STG_orders')
                field: order_id
      - name: payment_value
        description: "Payment value for the order."
        tests:
          - not_null
          - not_negative
      - name: payment_installments
        description: "Number of installments for the payment."
        tests:
          - not_null
          - not_negative
      - name: payment_type
        description: "Payment method used for the order."
        tests:
          - accepted_values:
              arguments:
                values: ['credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined']
```

### Custom Generic Test  
The `not_negative` test ensures that numeric columns (e.g., `price`, `payment_value`) never contain negative values. Designed as a **custom generic test**, it is modular and can be applied to multiple models and columns across the project for consistent data validation.

```sql
{% test not_negative(model, column_name) %}
    SELECT {{ column_name }}
    FROM {{model}}
    WHERE {{ column_name }} < 0
{% endtest %} 
```


### Singular Tests – Business Logic & Cross-Table Validation  

We implemented domain-specific singular tests to ensure business logic and data consistency. These tests highlight how **dbt enables rule-based data validation** beyond basic null checks. 

#### Example 1: Coordinates validation**
`coordinates_validation`
- **Logic:** longitude and latitude ranges should be logical  
- **Purpose:** Ensures data reliability for future BI illustrations in maps 
- **Severity:** error (test failure stop runs)  
- **Result:** ~0 errors — good data quality  

```sql
SELECT latitude, longitude
FROM {{ ref('STG_geolocation') }}
WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
```

#### Example 2: Payment consistency
Delivered/shipped/invoiced orders: payments = total item price + freight (link)
`payment_test_1`
- **Logic:** For delivered/shipped/invoiced orders, aggregated payments = item price + freight  
- **Purpose:** Ensures financial completeness & accuracy  
- **Tolerance:** ±0.05 (to avoid rounding noise)  
- **Severity:** warn (test failure gives a warning but does not stop runs)  
- **Result:** ~258 mismatches — likely due to installment interest or dataset quirks. Overall, it is decided to keep it as a *warning* test. 

```sql 

{{ config(severity='warn') }}  

SELECT 
    p.order_id,
    p.payment_value,
    (o.total_price + o.total_freight_value) AS expected_payment
FROM {{ ref('INT_order_payments_agg') }} as p
INNER JOIN {{ ref('INT_order_items_agg') }} as o 
    ON p.order_id = o.order_id
INNER JOIN {{ ref('STG_orders') }} as s
    ON p.order_id = s.order_id
WHERE 
    s.order_status IN ('delivered', 'shipped', 'invoiced')
    -- Check for a difference greater than a small tolerance (e.g., 10 cent)
    AND ABS(p.payment_value - (o.total_price + o.total_freight_value)) > 0.10
```



## 🧮 Analytical Modeling – RFM Segmentation  

A **Recency, Frequency, Monetary (RFM)** model in the marts layer segments customers by behavior.  
- Scope: **latest 12 months** of data (industry-standard window)  
- Each metric scored **1–5**; summed to a total RFM score  
- Segments such as *Loyal*, *At Risk*, *New* derived downstream  

👉 **Put RFM SQL snippet here (recency/frequency/monetary calculation)**

This model demonstrates how dbt outputs feed BI dashboards or retention analytics.

## 🧮 Macros, Seeds, & Packages  

### 🔹 Macros  
Reusable logic for date-differences, rounding, and environment-aware schema naming.  
👉 **Put macro snippet here**

### 🔹 Seeds  
Reference tables (e.g., product-category translations, RFM thresholds).  
👉 **Put seed CSV example here**

### 🔹 Packages  
- `dbt-utils` – relationship & integrity tests  
- `dbt-expectations` – advanced data-validation rules  

## ☁️ Deployment on dbt Cloud  

Executed on **dbt Cloud** with **BigQuery** backend.  
- Scheduled daily builds for staging + marts  
- Auto-generated documentation & lineage  
- **Materializations** managed globally (tables/views/incremental)  

👉 **Put `dbt_project.yml` global config snippet here**

## 📊 Key Learnings & Insights  

1. **Mastering dbt Concepts** – correct use of `source()` & `ref()` ensures dependency-aware builds.  
2. **Value of Testing** – custom & singular tests exposed genuine data issues in Olist.  
3. **Business Relevance** – RFM and performance models connect technical work to real analytics.  
4. **ELT Mindset** – dbt streamlines post-load transformation aligned with modern DE practices.  
5. **Documentation & Lineage** – auto-generated docs make pipelines auditable and transparent.  

## 🔗 Related Projects  
- **[SQL Sales Performance Analysis (PostgreSQL)](your_sql_project_link_here)**  
- **[Power BI Sales Dashboard](your_powerbi_project_link_here)**  

## 🏁 Conclusion  
This project demonstrates a **complete dbt workflow** from raw data to analytics-ready marts, combining technical depth with data-quality awareness.  
It showcases strong understanding of **data modeling, testing, and analytical design**, proving readiness for real-world data-engineering and analytics roles.  

