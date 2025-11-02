<h1 align="center">  dbt Project - Olist Dataset </h1>  
<p align="center">
  <img src="https://github.com/user-attachments/assets/f44ab50e-220b-42be-a0e0-91068f7d5cdf" width="2000">
</p>

## 🧩 About the Project  
This project transforms and validates the Olist Brazilian E-commerce dataset using **dbt (Data Build Tool)** with **Google BigQuery** as the data warehouse. After loading raw CSV files into BigQuery, dbt manages the **complete ELT workflow** — covering **data modeling**, **testing**, **documentation**, and **analytics readiness**.

🔗 **Dataset:** The data is available on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The project was initially developed locally using **dbt-core** in **VS Code**, connected to **BigQuery** through a **service account key**. After completing the development, the **GitHub repository** was linked to **dbt Cloud** to execute transformations and explore the **dbt Catalog**.

## 🎯 Objectives
The strength of **dbt** lies in providing a scalable, version-controlled development lifecycle that ensures consistency in how data is modeled, tested, and deployed across environments and teams. This project explores the end-to-end process of building a **modular and maintainable** dbt project by following the objectives below:

- 🧱 **Data Modeling & Architecture**
  - Implement a clean, layered structure: **staging** → **intermediate** → **marts**
  - Build star-schema **fact** and **dimension** tables for analytics
  - Create standalone analytical models such as **RFM segmentation**, **cohort retention**, and customer/seller performance

- ✅ **Data Quality & Testing**
  - Apply **generic**, **custom generic**, and **singular tests** to ensure data integrity and validate business logic

- ⚙️ **Scalability & Maintainability**
  - Use **sources**, **refs**, and **macros** to ensure modularity and dependency-aware builds
  - Demonstrate different configurations such as **materializations** or test **severity**

- ☁️ **Deployment & Documentation**
  - Automate transformations and testing on **dbt Cloud** + **BigQuery**
  - Generate interactive documentation and **DAG (Directed Acyclic Graph)** lineage visualizations for transparency and reproducibility using **dbt Catalog**


## 🏗️ Project Architecture  

The picture below shows the dbt project folder structure. 

<p align="center">
  <img src="https://github.com/user-attachments/assets/1befca44-0fda-4b36-b7da-07c2a86e639a" width="1000">
</p>


## ⚙️ Data Source Configuration  
The source datasets are defined in `_sources.yml`, referencing all raw tables located in the `rawdata` schema of the `olist-ecommerce-2025` dataset in **BigQuery**.

<p align="center"><i>Excerpt from `_sources.yml`</i></p>

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
In this project, dbt **models** are built in three standard layers: 
- **Staging Layer** – Clean column names, standardize datatypes, rename IDs, etc. 
- **Intermediate Layer** – joins, aggregations, intermediate tables/views, etc.
- **Marts Layer** – Final joins, aggregations, etc. for BI and reporting.  (RFM, seller performance, delivery reliability).

Note that a **seed** CSV is used to add full province names to desired tables alongside their abbreviations. Moreover, a **macro** created for automatic definition of model **schema** based on folder's name in the models directory in order to organize models in the BigQuery dataset. 

<p align="center"><i>`schema.sql`</i></p>

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

## ✅ dbt Tests for Data Quality & Integrity  
### 1️⃣ Generic Tests  
In this project, generic tests ensure fundamental data integrity. Defining **unique** and **not_null** tests for primary keys is essential, while **relationships** tests validate foreign key references. For columns with a limited set of valid categorical values (e.g., `order_status`), **accepted_values** tests are applied to enforce consistency.

<p align="center"><i>Excerpt from `_staging.yml`</i></p>

```yml
version: 2
models:
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
      - name: payment_type
        description: "Payment method used for the order."
        tests:
          - accepted_values:
              arguments:
                values: ['credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined']
```

### 2️⃣ Custom Generic Test  
The `not_negative` test ensures that numeric columns (e.g., `price`, `payment_value`) never contain negative values. As a **custom generic test**, it is modular and reusable across multiple models and columns.

<p align="center"><i>`not_negative.sql`</i></p>

```sql
{% test not_negative(model, column_name) %}
    SELECT {{ column_name }}
    FROM {{model}}
    WHERE {{ column_name }} < 0
{% endtest %} 
```

### 3️⃣ Singular Tests – Business Logic & Cross-Table Validation  

We implemented domain-specific singular tests to ensure business logic and data consistency. These tests highlight how **dbt enables rule-based data validation** beyond basic null checks. 

#### Example 1: Coordinates validation
- **Logic:** longitude and latitude ranges should be logical  
- **Purpose:** Ensures data reliability for future BI illustrations in maps 
- **Severity:** ❌ `error` 
- **Result:** ~0 errors — good data quality  
 
<p align="center"><i>`coordinates_validation.sql`</i></p>

```sql
SELECT latitude, longitude
FROM {{ ref('STG_geolocation') }}
WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
```

#### Example 2: Payment consistency
- **Logic:** For delivered/shipped/invoiced orders, aggregated payments = item price + freight  
- **Purpose:** Ensures financial completeness & accuracy  
- **Tolerance:** ±0.05 (to avoid rounding noise)  
- **Severity:** ⚠️ `warn`
- **Result:** ~258 mismatches were detected. Logically, the test should use `error` severity to fail the run, but due to quirks in the dataset (e.g., installment interest and other inconsistencies), it was set to `warn` to allow the pipeline to continue while still flagging potential issues.

<p align="center"><i>`payment_test_1.sql`</i></p>
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




