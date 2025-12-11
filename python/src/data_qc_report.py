import numpy as np
import pandas as pd
from . import sql_queries as q
from .utils import fetch_data_from_bq
import json
from pathlib import Path

def perform_data_qc(df, df_name="DataFrame"):
    """
    Performs QC, prints the report, and returns the data as a dictionary for JSON.
    This version includes the fix for NumPy types (np.float64) in JSON output.
    """
    report = {}

    print("="*80)
    print(f"                        *** Quality Control Report for {df_name} ***")
    
    # --- Table Shape and Duplicates ---
    print("\n### Table Information Overview ###")
    n_rows = len(df)
    n_cols = len(df.columns)

    # Populate report dictionary with shape/duplicate data
    report['df_name'] = df_name
    report['total_rows'] = int(n_rows)
    report['total_columns'] = int(n_cols)
    n_dup_rows = df.duplicated().sum()
    report['total_duplicated_rows'] = int(n_dup_rows)
    
    print(f"Total Rows: {n_rows:,}")
    print(f"Table Name: {df_name}")
    print(f"Total Columns: {n_cols}")
    print(f"Total Duplicated Rows: {n_dup_rows}")
    print("-" * 80)

    # --- Nulls and Column Information Overview ---
    print("\n#### Null Value Analysis (For Each Columns) ####")
    report['column_qc'] = {}
    
    null_counts = df.isnull().sum()
    null_percents = (null_counts / n_rows) * 100
    
    # Summary for printing
    null_summary = pd.DataFrame({
        'Dtype': df.dtypes,
        'Null Count': null_counts,
        'Null Percent': null_percents.round(3).astype(str) + '%' 
    }).sort_values(by='Null Count', ascending=False)

    print(null_summary)
    
    # Populate report dictionary with clean types for JSON serialization
    for col in df.columns:
        percent_value = null_percents[col]
        
        # Clean up null_percent for JSON (FIX: Explicitly cast to standard float)
        if pd.notna(percent_value):
            null_percent_clean = round(float(percent_value), 3)
        else:
            null_percent_clean = 0.0
            
        report['column_qc'][col] = {
            'dtype': str(df[col].dtype),
            'null_count': int(null_counts[col]),
            'null_percent': null_percent_clean 
        }

    print("-" * 80)

    # --- Basic Statistics for Numeric Columns ---
    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
    
    if numeric_cols:
        print("\n#### Basic Statistics (Numeric Columns) ####")
        # Ensure conversion to standard float for JSON compatibility
        stats = df[numeric_cols].agg(['min', 'median', 'max']).T.astype(float) 
        print(stats)
        
        for col in numeric_cols:
            # Add min, median, max to the column's entry in the report
            report['column_qc'][col].update({
                'min': float(stats.loc[col, 'min']),
                'median': float(stats.loc[col, 'median']),
                'max': float(stats.loc[col, 'max'])
            })
    else:
        print("\n* No Numeric Columns Found")
    print("-" * 80)
    
    # --- Value Counts for Categorical Columns ---
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

    if categorical_cols:
        print("\n#### Value Counts (Categorical Columns) ####")
        for col in categorical_cols:
            print(f"\nColumn: {col}")
            counts = df[col].value_counts(dropna=False)
            
            unique_count = len(counts)
            top_counts = counts.head(3).to_dict()
            
            # Populate report dictionary
            report['column_qc'][col]['unique_count'] = unique_count
            report['column_qc'][col]['top_3_values'] = top_counts
            
            print(counts.head(3)) 
            if unique_count > 3:
                print(f"... and {unique_count - 3} more unique values.")
    else:
        print("\n* No Categorical/Object Columns Found")
    
    print("\n" + "="*80)
    
    return report

# Function to save QC report as JSON

def save_qc_report(report: dict, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=4)
    print(f"[QC] Saved QC report → {path}")


# Perform QC on all relevant dataframes (all raw data tables)

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # points to OLIST/

queries_to_process = {
    # Output Name (Key) : SQL Query Variable (Value)
    "CUSTOMERS": q.GET_CUSTOMERS,
    "GEOLOCATION": q.GET_GEOLOCATION,
    "ORDER_ITEMS": q.GET_ORDER_ITEMS,
    "ORDER_PAYMENTS": q.GET_ORDER_PAYMENTS,
    "ORDER_REVIEWS": q.GET_ORDER_REVIEWS,
    "ORDERS": q.GET_ORDERS,
    "PRODUCTS": q.GET_PRODUCTS,
    "SELLERS": q.GET_SELLERS
}

for df_clean_name, sql_query_name in queries_to_process.items():
    qc = perform_data_qc(fetch_data_from_bq(sql_query_name), df_name=df_clean_name)
    save_qc_report(qc, PROJECT_ROOT / "python" / "outputs" /"QC_Reports" / f"{df_clean_name}.json")