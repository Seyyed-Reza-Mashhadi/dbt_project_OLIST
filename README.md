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
  - Create standalone analytical models such as **customer RFM segmentation**, **cohort retention**, and customer/seller performance

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
  <img src="https://github.com/user-attachments/assets/1befca44-0fda-4b36-b7da-07c2a86e639a" width="800">
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

## 🧩 Modeling, Testing & Transformation Workflow  
In dbt, data transformation, testing, and documentation are tightly integrated — not sequential. As each model is created or updated by each transformation step, it is immediately tested, and becomes part of the **DAG (Directed Acyclic Graph)**. 

### 🧱 Model Layers
#### Overview
- **Staging Layer** – Renaming column names, standardize datatypes, etc. 
- **Intermediate Layer** – Performs joins, aggregations, and logic transformations between staging and marts.
- **Marts Layer** – Produces analytics-ready tables for reporting and BI, including both **star schema** models and **standalone analytical models** such as RFM segmentation, seller performance, and delivery reliability.

A **seed** CSV enriches the models with full province names (linked by province abbreviation). Additionally, a **macro** automates **schema** assignment in BigQuery based on each model’s folder location — ensuring organized and scalable dataset management. 

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
#### A Look Into the Mart Layer
The marts layer contains both dimensional and analytical models:
- **Fact and Dimension Tables** – Form the basis of a star schema, including a `date` dimension table 
- **standalone BI specific models:** Designed to answer concrete business questions and support dashboards (e.g., RFM segmentation, cohort retention, seller reliability).

  
**Example 1: RFM Segmentation Model**
This model segments customers using the classic **Recency, Frequency, Monetary (RFM)** framework:
- **Metrics:**
  - Recency (R): Days since last purchase
  - Frequency (F): Number of purchases
  - Monetary (M): Total spent
- **Scope:** Last 12 months of transactions in the dataset are considered
- Combined RFM score determines customer segments (*Champions*, *Loyal Customers*,*Potential Loyalists*, *At Risk*, *Lost*)

🔗 **File:** [RFM Segmentation Model](https://github.com/Seyyed-Reza-Mashhadi/dbt_project_OLIST/blob/master/olist/models/mart/BI_customer_rfm.sql)

**Example 2: Cohort Analysis Model**
This model groups customers into **cohorts** based on their **first purchase date** and tracks **customer retention rate** and spending across time periods. It evaluates both individual cohort performance and weighted averages to reveal overall customer lifecycle trends.

🔗 **File:** [Cohort Analysis Model](https://github.com/Seyyed-Reza-Mashhadi/dbt_project_OLIST/blob/master/olist/models/mart/BI_customer_cohorts.sql)

## ✅ Integrated Data Testing
Testing occurs alongside model development — ensuring every transformation maintains data quality before it’s used downstream.
### 1️⃣ Generic Tests  
Generic tests ensure fundamental data integrity. Defining **unique** and **not_null** tests for primary keys is essential, while **relationships** tests validate foreign key references. For columns with a limited set of valid categorical values (e.g., `order_status`), **accepted_values** tests are applied to enforce consistency.

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
    AND ABS(p.payment_value - (o.total_price + o.total_freight_value)) > 0.10 -- considering a small tolerance (e.g., 10 cent)
```

## ☁️ Deployment on dbt Cloud  

After completing and testing all dbt models locally using **dbt-core** in VS code, the project was deployed to **dbt Cloud**, connected directly to the GitHub repository for version control and continuous integration. The dbt Cloud environment is configured to use **Google BigQuery** as the data warehouse for computation and storage.

<p align="center"><i>Excerpt from `dbt_project.yml`</i></p>

```yml

name: 'olist'
version: '1.0.0'
profile: 'olist'
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
models:
  olist:
    staging:
      schema: staging
      +materialized: view
    intermediate:
      schema: intermediate
      +materialized: view
    mart:
      schema: mart
      +materialized: table
seeds:
  olist:
    brazil_states:
      file: seeds/brazil_states.csv
```
**Key Features in dbt Cloud:**
- **Scheduled Jobs:** Automate builds (e.g., dbt build, dbt test) on a defined cadence or trigger via CI/CD.

<p align="center">
  <img src="https://github.com/user-attachments/assets/f37f9df2-f815-4843-a604-20e9305dabeb" width="800">
</p>

- **Automatic Documentation:** dbt Cloud generates an interactive documentation site (dbt docs generate) including model descriptions, lineage graphs, and test results.

<p align="center">
  <img src="https://github.com/user-attachments/assets/47f5bcdf-5f0a-4541-88af-e42f9af8757a" width="1000">
</p>

- **Data Health & Monitoring:** Built-in data quality dashboards show the latest test outcomes with intuitive green/yellow/red status icons.

<p align="center">
  <img src="https://github.com/user-attachments/assets/e3732a69-2110-4ec8-bd5b-abb1c3f138ce" width="300">
</p>

-  **Directed Acyclic Graph (DAG)** visualizes the complete data flow and provides both data-level and column-level traceability, enabling effective troubleshooting, debugging, and ensuring data quality throughout the transformation pipeline.
<p align="center">
  <img src="https://github.com/user-attachments/assets/368aeac8-0f79-41b5-a47a-fd051eca20e1" width="800">
</p>

## 📊 Key Learnings & Insights  

1. **Mastering dbt Concepts** – correct use of `source()` & `ref()` ensures dependency-aware builds.  
2. **Value of Testing** – custom & singular tests exposed genuine data issues in Olist.  
3. **Business Relevance** – RFM and performance models connect technical work to real analytics.  
4. **ELT Mindset** – dbt streamlines post-load transformation aligned with modern DE practices.  
5. **Documentation & Lineage** – auto-generated docs make pipelines auditable and transparent.


<p align="center">
  <img src="https://github.com/user-attachments/assets/64ad3d8d-7520-404c-b77a-178a7f71e8f8" width="1000">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/8a237af7-5bdd-4c16-84d2-cacdb25920d8" width="1000">
</p>

## 🔗 Related Projects  
- **[SQL Sales Performance Analysis (PostgreSQL)](your_sql_project_link_here)**  
- **[Power BI Sales Dashboard](your_powerbi_project_link_here)**  

## 🏁 Conclusion  
This project demonstrates a **complete dbt workflow** from raw data to analytics-ready marts, combining technical depth with data-quality awareness.  
It showcases strong understanding of **data modeling, testing, and analytical design**, proving readiness for real-world data-engineering and analytics roles.  








