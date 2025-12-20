<p align="center">
  <img src="https://github.com/user-attachments/assets/a2dd997e-ad75-46b7-a76e-018b6e9e5bde" width="2000">
</p>



## 🧩 Project Summary  
This project transforms, validates, and demonstrates data analytics and reporting of the Olist Brazilian E-commerce dataset. After loading raw CSV files into BigQuery, dbt manages the **transformation** in the ELT workflow — covering **data modeling**, **testing**, **documentation**, and **analytics readiness**. Then, a **Python analytics package** that performs anomaly detection, KPI calculations, and constructing a proper prompt to be able to generate **AI / LLM**-driven report with actionable business insights. The generated AI narrative is embedded into Power BI alongside the dashboards. The most important thing is that the whole data cleaning, transformation, analytics, and report generation is **automated**, which means that the most updated dashboards and report with data-driven insights can be created easily and quickly.  


**Key ideas:**
- **Google BigQuery** serves as the data warehouse with powerful computation resource.
- **dbt (Data Build Tool)** provides the trusted, tested, version-controlled transformation foundation.
- A **Python** analytics layer consumes dbt-produced marts, runs deeper programmatic checks, and produces structured JSON summaries.
- A constrained **AI / LLM** layer (**OpenAI** + **Google Gemini**) synthesizes explainable, auditable narrative reports from these summaries.
- Everything is automated, ...

🔗 **Dataset:** The data is available on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## 🏗️ Project Architecture
The picture below shows the dbt project folder structure. 

```graphql
OLIST/
├─ dbt-olist/                     # dbt transformation and testing
│  ├─ models/
│  ├─ tests/
│  └─ ...
├─ python/                        # Python analytics + AI layer
│  ├─ src/
│  ├─ outputs/
│  └─ ...
├─ README.md                      # project documentation 
├─ .env                           # file containting services account details, API keys (excluded from github repository)
├─ Run_Pipeline.bat               # batch to the project by a double click
│ ...
```
 
## dbt: Data Testing & Transformation

The project was initially developed locally using **dbt-core** in **VS Code**, connected to **BigQuery** through a **service account key**. After completing the development, the **GitHub repository** was linked to **dbt Cloud** to execute transformations and explore the **dbt Catalog**. 

### 🏗️ Architecture

```graphql
OLIST/
├─ dbt-olist/                         # dbt transformation and testing
│  ├─ analysis/
│  ├─ dbt_project.yml
│  ├─ profiles.yml
│  ├─ models/
│  │  ├─ staging/                            # staging layer models
│  │  │  ├─ _sources.yml                     # defining the sources        
│  │  │  ├─ _staging.yml                     # defining model configs, tests, etc.
│  │  │  ├─ STG_orders.sql
│  │  │  ├─ ...
│  │  ├─ intermediate/                       # intermediate layer models
│  │  │  ├─ INT_order_items_agg.sql
│  │  │  ├─ ...
│  │  ├─ mart/                               # mart layer models
│  │  │  ├─ _mart.yml                        # defining model configs, tests, etc.
│  │  │  ├─ Fact_orders.sql        
│  │  │  ├─ ...
│  ├─ tests/
│  │  ├─ tests/
│  │  │  ├─ not_negative.sql                 # custom generic test
│  │  ├─ coordinates_validation.sql          # singular test 1
│  │  ├─ delivery_date_check.sql             # singular test 2
│  │  ├─ payment_test_1.sql                  # singular test 3
│  │  ├─ ...
│  ├─ macros/
│  │  ├─ schema.sql                          # macro to define schema
│  └─ ...
│ ...
```

### 🎯 Objectives
The strength of **dbt** lies in providing a scalable, version-controlled development lifecycle that ensures consistency in how data is modeled, tested, and deployed across environments and teams. This project explores the end-to-end process of building a **modular and maintainable** dbt project by following the objectives below:

- 🧱 **Data Modeling & Architecture**
  - Implement a clean, layered structure: **staging** → **intermediate** → **marts**
  - Build star-schema **fact** and **dimension** tables for analytics
  - Create standalone analytical models such as **customer RFM segmentation**, **cohort retention**, and customer/seller performance.

- ✅ **Data Quality & Testing**
  - Apply **generic**, **custom generic**, and **singular tests** to ensure data integrity and validate business logic

- ⚙️ **Scalability & Maintainability**
  - Use **sources**, **refs**, and **macros** to ensure modularity and dependency-aware builds
  - Demonstrate different configurations such as **materializations** or test **severity**

- ☁️ **Deployment & Documentation**
  - Automate transformations and testing on **dbt Cloud** + **BigQuery**
  - Generate interactive documentation and **DAG (Directed Acyclic Graph)** lineage visualizations for transparency and reproducibility using **dbt Catalog**

### ⚙️ Data Source Configuration  
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

### 🧩 Modeling, Testing & Transformation Workflow  
In dbt, data transformation, testing, and documentation are tightly integrated rather than sequential. Each model, once created or updated, it is immediately tested and incorporated into the project's **Directed Acyclic Graph (DAG)**. 

<p align="center"><i>DAG of Entire Project</i></p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/f44ab50e-220b-42be-a0e0-91068f7d5cdf" width="2000">
</p>


#### 🧱 Model Layers
##### Overview
- **Staging Layer** – Renaming column names, standardize datatypes, etc. 
- **Intermediate Layer** – Performs joins, aggregations, and logic transformations between staging and marts.
- **Marts Layer** – Produces analytics-ready tables for reporting and BI, including both **star schema** models and **standalone analytical models** such as RFM segmentation, seller performance, and delivery reliability.

A **seed** CSV enriches the models with full province names (linked by province abbreviation). Additionally, the `schema.sql` **macro** ensures schema naming consistency and automates dataset organization inside BigQuery. 

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
##### A Look Into the Mart Layer
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

<p align="center"><i>Executing Cohort Analysis model build with dbt Core in VS Code</i></p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/609b0059-56c7-40ac-9a29-3cadb5cff9ed" width="800">
</p>

### ✅ Integrated Data Testing
Testing occurs alongside model development — ensuring every transformation maintains data quality before it’s used downstream.
#### 1️⃣ Generic Tests  
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

#### 2️⃣ Custom Generic Test  
The `not_negative` test ensures that numeric columns (e.g., `price`, `payment_value`) never contain negative values. As a **custom generic test**, it is modular and reusable across multiple models and columns.

<p align="center"><i>`not_negative.sql`</i></p>

```sql
{% test not_negative(model, column_name) %}
    SELECT {{ column_name }}
    FROM {{model}}
    WHERE {{ column_name }} < 0
{% endtest %} 
```

#### 3️⃣ Singular Tests – Business Logic & Cross-Table Validation  

We implemented domain-specific singular tests to ensure business logic and data consistency. These tests highlight how **dbt enables rule-based data validation** beyond basic null checks. 

##### Example 1: Coordinates validation
- **Logic:** longitude and latitude ranges should be logical  
- **Purpose:** Ensures data reliability for future BI illustrations in maps 
- **Severity:** ❌ `error` 
- **Result:** ~0 errors — good data quality  
 
<p align="center"><i>`coordinates_validation.sql`</i></p>

```sql
{{ config(store_failures = true) }} 
SELECT latitude, longitude
FROM {{ ref('STG_geolocation') }}
WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures
```

##### Example 2: Payment consistency
- **Logic:** For delivered/shipped/invoiced orders, aggregated payments = item price + freight  
- **Purpose:** Ensures financial completeness & accuracy  
- **Tolerance:** ±0.05 (to avoid rounding noise)  
- **Severity:** ⚠️ `warn`
- **Result:** ~258 mismatches were detected. Logically, this test should have an `error` severity to fail the run, but due to known data quirks (e.g., installment interest), it was set to `warn` to allow the pipeline to continue while still flagging potential issues.

<p align="center"><i>`payment_test_1.sql`</i></p>

```sql 
{{ config(severity='warn', store_failures = true) }}  

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
    AND ABS(p.payment_value - (o.total_price + o.total_freight_value)) > 0.10 -- considering a small tolerance of 10 cents
LIMIT 1000  -- Cap the materialization to avoid excessive data storing in case of widespread failures
```

### ☁️ Deployment on dbt Cloud  

After completing and testing all dbt models locally using **dbt-core** in VS code, the project was deployed to **dbt Cloud**, connected directly to the GitHub repository for version control and continuous integration. The dbt Cloud environment is configured to use **Google BigQuery** as the data warehouse for computation and storage.

<p align="center"><i>`dbt_project.yml`</i></p>

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
  <img src="https://github.com/user-attachments/assets/f37f9df2-f815-4843-a604-20e9305dabeb" width="700">
</p>

- **Automatic Documentation:** dbt Cloud generates an interactive documentation site (dbt docs generate) including model descriptions, lineage graphs, and test results.

<p align="center">
  <img src="https://github.com/user-attachments/assets/47f5bcdf-5f0a-4541-88af-e42f9af8757a" width="700">
</p>

- **Data Health & Monitoring:** Built-in data quality dashboards show the latest test outcomes with intuitive green/yellow/red status icons.

<p align="center">
  <img src="https://github.com/user-attachments/assets/e3732a69-2110-4ec8-bd5b-abb1c3f138ce" width="300">
</p>

-  The **Directed Acyclic Graph (DAG)** provides both data- and column-level traceability, supporting efficient debugging and ensuring data quality across the pipeline.

<p align="center">
  <img src="https://github.com/user-attachments/assets/368aeac8-0f79-41b5-a47a-fd051eca20e1" width="700">
</p>



## Python Analytics & AI (LLM) Layer

This section describes the Python modules, how they consume dbt marts in BigQuery, produce JSON artifacts (QC, anomalies, analysis), build an LLM-safe context, and request AI insights from OpenAI and Google Gemini.

📦 Python package structure
```graphql
OLIST/
├─ python/                          # Python analytics + AI layer
│  ├─ config/
│  ├─ src/
│  │  ├─ __init__.py
│  │  ├─ utils.py                   # BigQuery client & helpers
│  │  ├─ queries.py                 # SQL strings / queries to run and load from BigQuery
│  │  ├─ raw_data_qc.py             # General QC checks of raw data + summary outputs to JSON
│  │  ├─ anomaly_detection.py       # Anomaly detection + summary outputs to JSON
│  │  ├─ analysis.py                # KPI/aggregation for different analytics + summary outputs to JSON
│  │  ├─ context_builder.py         # Merges JSON outputs + constructs prompt/context
│  │  └─ ai_generator.py            # Calls OpenAI & Gemini + saving AI reports in text format
│  ├─ scripts/
│  │  ├─ __init__.py
│  │  ├─ run_all.py                 # orchastation 
│  ├─ notebooks/                    # notebooks for data exploration
│  ├─ outputs/                      # directory to save JSON files and AI-report
│  └─ ...
│ ...
```



utils.py — minimal example (no secrets)
code snippet 
queries.py — keep SQL separated
code snippet 
...

Responsible AI / LLM practices used

Grounding: LLMs only read validated JSON summaries (no raw tables).

Constraints in prompt: explicit instruction to avoid inventing facts — "Do not assume or hallucinate; reference only the supplied numbers and labeled anomalies."

Determinism: low temperature and structural JSON output to reduce randomness.

Audit trail: all context files and LLM outputs are saved to outputs/ for reproducibility and audit.

Dual-model comparison: run both OpenAI and Gemini, compare outputs for coherence and factuality, then pick the best.

Power BI

Import analysis_summary.json or connect Power BI directly to BigQuery marts.

Add the final AI narrative into a Power BI narrative page (text box or HTML container).

Tech Stack (concise)

Data Warehouse: Google BigQuery

Transformations: dbt (dbt-core + dbt Cloud)

Analysis: Python (pandas, google-cloud-bigquery)

AI / LLM: OpenAI, Google Gemini (Gemini) — constrained prompt design

BI: Power BI (dashboards + narrative page)

CI / Repo: GitHub + dbt Cloud

Conclusion (unified)

This repository is a single, end-to-end analytics project built on top of dbt transformations. dbt is the authoritative source for cleaned, tested, analytics-ready tables; the Python layer extends dbt with programmatic data quality checks, anomaly detection, KPI aggregation, and AI-driven narrative generation. The LLM layer is used responsibly — to interpret validated metrics and to generate an executive-ready narrative embedded into Power BI. This design demonstrates how analytics engineering, reproducible data science, and constrained AI/LLM usage can be integrated to deliver trustworthy, decision-ready insights.



















## 🧩 Final Remarks

This project highlights dbt’s strength as a **transformation framework**, enabling modular, tested, and transparent data pipelines. Even as a static project, it demonstrates the complete transformation workflow — from staging and intermediate logic to analytical marts — emphasizing the “T” in ELT.

While **freshness** and **incremental models** were not needed here, they remain essential for real-time or production pipelines. dbt Cloud’s **Catalog** and **Lineage** features provided deep visibility into data dependencies and column-level transformations.

Custom testing exposed **over 200 mismatches** between order payments and product price totals, revealing real-world inconsistencies in the Olist dataset — likely due to installment payments with interest, taxes, or other adjustments.

Analytical models in the mart layer added value for analytics and reporting. For instance, **Cohort Retention** and **RFM Segmentation** (visualized in Power BI, shown below) show strong acquisition but weak retention: **fewer than 1% of customers** repurchase after their first month. Only **~12.5% of customers** qualify as loyal or champion segments, while most revenue comes from potential loyalists. This means that OLIST is functioning as a **one-time, high-value purchase model, with revenue highly dependent on new customer acquisition** and suffering from a near-total failure to generate repeat business. 

Overall, this dbt project bridges data engineering and analytics, demonstrating how clean, tested transformations can power reliable business insights and BI-ready datasets.


<p align="center">
  <img src="https://github.com/user-attachments/assets/f1156c89-3260-4b1b-a0e1-9812c8713c49" width="1000">
</p>





















