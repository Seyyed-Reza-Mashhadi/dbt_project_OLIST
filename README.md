<p align="center">
  <img src="https://github.com/user-attachments/assets/a2dd997e-ad75-46b7-a76e-018b6e9e5bde" width="2000">
</p>


## 🧩 Project Summary  

This project demonstrates a **fully automated, end-to-end analytics and reporting platform** built on the Olist Brazilian E-commerce dataset.
Raw CSV files are loaded into **Google BigQuery**, where **dbt (Data Build Tool)** manages the complete **transformation layer** of the ELT workflow — including **data modeling**, **testing**, **documentation**, and **analytics readiness**.  
On top of the dbt-produced marts, a **Python analytics package** performs deeper programmatic checks, anomaly detection, KPI calculations, and constructs a controlled prompt to generate **AI / LLM–driven narrative reports** (using **OpenAI** and **Google Gemini**) with actionable business insights.  
The generated AI narrative is then embedded into **Power BI** alongside interactive dashboards.

The key design principle of this project is **automation**: data cleaning, transformation, analytics, and report generation are fully automated, enabling fast regeneration of up-to-date dashboards and data-driven insights with minimal manual effort.

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
├─ .env                           # service account & API keys (excluded from GitHub)
├─ Run_Pipeline.bat               # one-click pipeline execution
└─ ...
```
 
## dbt: Data Testing & Transformation

The dbt component of this project was initially developed locally using **dbt-core** in **VS Code**, connected to **BigQuery** through a **service account key**. After completing the development, the **GitHub repository** was linked to **dbt Cloud** to execute transformations and explore the **dbt Catalog**. 

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
- **Result:** ~258 mismatches were detected. Logically, this test should have an `error` severity to fail the run, but due to known data quirks (e.g., installment interest, taxes, etc.), it was set to `warn` to allow the pipeline to continue while still flagging potential issues.

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

This layer builds directly on top of dbt-produced marts in BigQuery and extends the platform with programmatic analytics, anomaly detection, and AI-assisted narrative reporting.
Its role is not to replace dbt, but to consume trusted dbt outputs and generate higher-level analytical artifacts and executive-friendly insights.
All Python modules read exclusively from analytics-ready dbt models, ensuring that downstream logic operates on tested, documented, and version-controlled data. The only exception is the module that examines raw data quality.

This section describes the Python modules, how they consume dbt marts in BigQuery, produce JSON artifacts (QC, anomalies, analysis), build an LLM-safe context, and request AI insights from OpenAI and Google Gemini.

### 📦 Python package structure
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

#### `utils.py` — BigQuery connectivity & helpers
This module centralizes BigQuery connectivity and shared utility functions, ensuring consistent authentication and reuse across analytics modules. 

```py
import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import bigquery_storage

# --- Global client cache (Singletons) ---
_bq_client = None
_bq_storage_client = None
def get_bq_client():
    """
    Initializes and caches BigQuery clients using environment variables.
    """
    global _bq_client, _bq_storage_client
    
    if _bq_client is None or _bq_storage_client is None:
        try:
            basedir = Path(__file__).resolve().parents[2]
            load_dotenv(basedir / '.env')
            _bq_client = bigquery.Client()   
            _bq_storage_client = bigquery_storage.BigQueryReadClient()
            print("✅ BigQuery and Storage clients initialized.")
        except Exception as e:
            print("\n❌ FATAL ERROR: Could not initialize BigQuery clients.")
            print("👉 Check GOOGLE_APPLICATION_CREDENTIALS in your .env file.")
            print(f"👉 Error details: {e}\n")
            return None, None
    return _bq_client, _bq_storage_client

def fetch_data_from_bq(sql_query):
    """
    Runs a query and returns a Pandas DataFrame using the high-speed Storage API.
    """
    client, storage_client = get_bq_client()
    if client is None:
        print("🛑 Fetch failed: Clients not initialized.")
        return None
    try:
        query_job = client.query(sql_query)
        df = query_job.to_dataframe(bqstorage_client=storage_client)
        mb_processed = query_job.total_bytes_processed / (1024**2)
        print(f"✔️ Query successful. Scanned {mb_processed:.2f} MB. Loaded {len(df)} rows.")
        return df
    except Exception as e:
        print("\n--- ⚠️ BIGQUERY QUERY FAILED ---")
        print(f"Error: {e}")
        print(f"Check your SQL syntax in sql_queries.py.")
        print(f"Failing Query Snippet: {sql_query.strip()[:100]}...\n")
        return None
```

#### `queries.py` — Analytics queries
This is where all SQL queires (used in the python scripts for analytics) live. 

```py
# SQL Queries for Olist Analytics

# Star schema fact and dimension tables

GET_FACT_ORDERS = """
SELECT * FROM `olist-ecommerce-1234321.mart.FACT_orders`
"""
...

### ADDITIONAL ANALYTICS QUERIES

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
...
```


#### `raw_data_qc.py` — High-level QC summaries

This module performs **lightweight, descriptive quality checks** (null counts, duplicates, basic distributions) and outputs JSON summaries.

Note: **Critical data quality enforcement is handled in dbt** via tests.
This Python QC layer is intentionally limited to **reporting and monitoring**, not enforcement.

#### `anomaly_detection.py` — Detection of High/Low Anomalies In Data

This module detects unusual behavior in key metrics (e.g., spikes in revenue, drops in order volume) using statistical thresholds and rolling comparisons. Two different methods are used based on data distribution (IQR and Z-SCORE) to be able to provide meaningful anomalies for both normal and non-normal distributions.

Detected anomalies are explicitly labeled and quantified, then exported as structured JSON.

SCREENSHOT FROM ANOMALY DETECTION

#### `analysis.py` — KPI & analytical summaries
This module computes core KPIs and...
The results are stored in a machine-readable JSON summary, designed specifically for downstream AI consumption.

SCREENSHOT FROM ANALYSIS RESULTS

#### `context_builder.py` & `ai_generator.py` — AI / LLM reporting layer

These modules transform analytical JSON outputs into a controlled, LLM-safe context and generate a business-facing narrative report. 
- Responsible AI / LLM practices
- Grounded inputs
LLMs receive only validated JSON summaries — never raw tables.

Strict prompt constraints
Prompts explicitly prohibit assumptions or hallucinations:

“Use only the supplied metrics and labeled anomalies. Do not invent explanations.”

Determinism
Low temperature and structured prompts minimize randomness.

Auditability
All prompts, contexts, and AI outputs are saved to /outputs for traceability.

Dual-model validation
Reports are generated using OpenAI and Google Gemini, compared for coherence and factual consistency, and the best result is retained.

#### run_all.py — Pipeline orchestration

This script executes the entire analytics and AI pipeline in sequence:
1. Load dbt mart data from BigQuery
2. Run QC checks
3. Detect anomalies
4. Generate KPI summaries
5. Build LLM context
6. Generate AI narrative report
This enables one-click regeneration of insights after any dbt update.


```py
import os
from pathlib import Path
from dotenv import load_dotenv
from src.raw_data_qc import run_raw_data_qc            # Step 1
from src.anomaly_detection import run_anomaly_detection    # Step 2a
from src.analysis import run_analysis              # Step 2b
from src.context_builder import run_context_builder   # Step 3
from src.ai_generator import run_ai_generator  # Step 4

load_dotenv()   # Load environment variables (API Keys, BQ Path)
def main():
    print("🚀 --- STARTING OLIST AI-ANALYTICS PIPELINE --- 🚀")
    print("="*50)
    print("\n🔍 STEP 1: Running Raw Data Quality Control...")
    try:
        run_raw_data_qc()
        print("✅ Data QC Complete.")
    except Exception as e:
        print(f"❌ QC Failed: {e}")
    print("\n📈 STEP 2: Detecting Anomalies in BigQuery Data...")
    try:
        run_anomaly_detection()
        print("✅ Anomaly Detection Complete.")
    except Exception as e:
        print(f"❌ Anomaly Detection Failed: {e}")
    print("\n📊 STEP 3: Computing Core Business Metrics...")
    try:
        run_analysis()
        print("✅ Business Analysis Complete.")
    except Exception as e:
        print(f"❌ Analysis Failed: {e}")
    print("\n📝 STEP 4: Building AI Context from JSON outputs...")
    try:
        run_context_builder()
        print("✅ AI Context built (business_context.txt created).")
    except Exception as e:
        print(f"❌ Context Builder Failed: {e}")
    print("\n✨ STEP 5: Generating AI Reports and Recommendations...")
    try:
        run_ai_generator()
        print("✅ AI Reports generated successfully.")
    except Exception as e:
        print(f"❌ AI Generation Failed: {e}")
    print("\n" + "="*50)
    print("🏁 PIPELINE FULLY EXECUTED!")
    print("📂 Check 'python/output/' for all reports and JSON files.")
    print("📊 Your Power BI dashboard is ready for refresh.")
if __name__ == "__main__":
    main()
```





## Adding AI-generated Report to Power BI

Import analysis_summary.json or connect Power BI directly to BigQuery marts.

Add the final AI narrative into a Power BI narrative page.










## 🧩 Conclusions

### General Remarks About The Project 

This project represents **a single, automated, end-to-end analytics project** with dbt-controlled transformation, python-based analytics and AI-augmented reporting.

Firstly, the project highlights **dbt**’s strength as a **transformation framework**, enabling modular, tested, and transparent data pipelines. It demonstrates the complete transformation workflow — from staging and intermediate logic to analytical marts — emphasizing the “T” in ELT process. Note that **freshness** and **incremental models** were not needed here for this static dataset. However, they remain essential for real-time or production-level dbt projects. 

Secondly, **Python layer** extends dbt with complex data analytics such as anomaly detection, KPI calculations, as well as AI-driven narrative generation. The LLM layer is used responsibly — to interpret validated metrics and to generate an executive-ready narrative embedded into Power BI. This is useful to get updated glamps of data as well as quick interpretations any time needed. (addind a sentence on why this is useful)

This design demonstrates how analytics engineering, reproducible data science, and constrained AI/LLM usage can be integrated to deliver trustworthy, decision-ready insights.


### Data-driven Insights About OLIST Dataset 

Complete data analytic summaries (JSON files) as well as AI-augmented reports (txt files) are available in this directory [this folder](https://github.com/Seyyed-Reza-Mashhadi/dbt_project_OLIST/tree/master/python/outputs). 

Here, customer cohort retention and FRM segmentation results are highlighted together with basic KPIs.


Analytical models in the mart layer added value for analytics and reporting. For instance, **Cohort Retention** and **RFM Segmentation** (visualized in Power BI, shown below) show strong acquisition but weak retention: **fewer than 1% of customers** repurchase after their first month. Only **~12.5% of customers** qualify as loyal or champion segments, while most revenue comes from potential loyalists. This means that OLIST is functioning as a **one-time, high-value purchase model, with revenue highly dependent on new customer acquisition** and suffering from a near-total failure to generate repeat business. 

<p align="center">
  <img src="https://github.com/user-attachments/assets/f1156c89-3260-4b1b-a0e1-9812c8713c49" width="1000">
</p>






















