import pandas as pd
import numpy as np
from . import sql_queries as q
from .utils import fetch_data_from_bq
import json
from pathlib import Path
from typing import List, Optional, Dict, Union

# --- 1. The Math Engines (Detection Core - No change) ---

def detect_iqr_outliers(df: pd.DataFrame, col_name: str, factor: float = 1.5) -> pd.DataFrame:
    """Detects outliers using Interquartile Range (IQR). Useful for non-normal distributions."""
    if df.empty or col_name not in df.columns:
        return pd.DataFrame()
    data = df[col_name]
    q1 = data.quantile(0.25)
    q3 = data.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - (factor * iqr)
    upper_bound = q3 + (factor * iqr)
    outliers = df[(data < lower_bound) | (data > upper_bound)].copy()
    outliers['method'] = 'IQR'
    outliers['lower_limit'] = lower_bound
    outliers['upper_limit'] = upper_bound
    outliers['anomaly_type'] = np.where(outliers[col_name] > upper_bound, 'Peak (High)', 'Valley (Low)')
    return outliers

def detect_zscore_outliers(df: pd.DataFrame, col_name: str, threshold: float = 3.0) -> pd.DataFrame:
    """Standard Z-Score method. Best for normal distributions."""
    if df.empty or col_name not in df.columns:
        return pd.DataFrame()
    data = df[col_name]
    mean = data.mean()
    std = data.std()
    if std == 0: 
        return pd.DataFrame()
    z_scores = (data - mean) / std
    outliers = df[np.abs(z_scores) > threshold].copy()
    outliers['method'] = 'Z-Score'
    outliers['z_score'] = z_scores[np.abs(z_scores) > threshold]
    outliers['anomaly_type'] = np.where(outliers['z_score'] > 0, 'Peak (High)', 'Valley (Low)')
    return outliers

# --- 2. Data Preparation and Mode Manager (metric_desc update) ---

def get_mode_name(mode: str) -> str:
    """Converts the analysis mode code to a readable name."""
    mapping = {
        'TIME_AGGREGATED': 'Time-Series',
        'TIME_RAW': 'Time-Series',
        'DISTRIBUTIONAL': 'Distributional (Static)'
    }
    return mapping.get(mode.upper(), mode)

def get_freq_name(freq_code: str | None) -> str:
    """Converts frequency code (D, W, M) to readable name or 'N/A'."""
    if freq_code is None:
        return 'N/A'
    mapping = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}
    return mapping.get(freq_code.upper(), freq_code)

def prepare_data_for_detection(
    df: pd.DataFrame, 
    value_col: str, 
    index_col: str,
    mode: str, 
    freq: str | None = None
) -> pd.DataFrame:
    """
    Manages data preparation based on the analysis mode, returning a standardized DataFrame.
    """
    df_copy = df.copy()
    
    # 1. Type Conversion
    if value_col not in df_copy.columns:
         raise ValueError(f"Value column '{value_col}' not found in DataFrame.")
    df_copy['value'] = df_copy[value_col].astype(float)
    
    # --- SCENARIO 1: DISTRIBUTIONAL (Static check) ---
    if mode.upper() == 'DISTRIBUTIONAL':
        ts_data = df_copy.rename(columns={index_col: 'index_id'})
        return ts_data[['index_id', 'value']].reset_index(drop=True)

    # --- SCENARIO 2 & 3: TIME-BASED ---
    
    if index_col not in df_copy.columns:
        raise ValueError(f"Time/Index column '{index_col}' not found for Time-based analysis.")
    df_copy['date'] = pd.to_datetime(df_copy[index_col])
    
    # --- SCENARIO 2: TIME_AGGREGATED ---
    if mode.upper() == 'TIME_AGGREGATED' and freq:
        agg_df = df_copy.groupby(pd.Grouper(key='date', freq=freq))['value'].sum().reset_index()
        agg_df.columns = ['date', 'value']
        agg_df['index_id'] = agg_df['date']
        return agg_df[agg_df['value'] > 0].reset_index(drop=True)

    # --- SCENARIO 3: TIME_RAW ---
    elif mode.upper() == 'TIME_RAW' or (mode.upper() == 'TIME_AGGREGATED' and not freq):
        ts_data = df_copy.rename(columns={index_col: 'index_id'})
        return ts_data[['index_id', 'value', 'date']][ts_data['value'] > 0].reset_index(drop=True)
    raise ValueError(f"Invalid mode '{mode}' or missing 'freq' for TIME_AGGREGATED mode.")

# --- 3. The Core Detection and Reporting (metric_desc update and improved print) ---

def anomaly_detection_core(
    ts_data: pd.DataFrame, 
    method: str, 
    mode_name: str, 
    freq_name: str,  # e.g., 'Daily', 'Weekly', 'Monthly', 'N/A'
    metric_desc: str 
) -> Dict:
    """
    Internal function to run detection and compile results using standardized data.
    """
    value_col = 'value'
    
    # ------------------ PRINT START ------------------
    print("-" * 50)
    print(f"Investigated Parameter: {metric_desc}")
    print(f"| Mode: {mode_name} | Frequency: {freq_name} | Method: {method}")
    print("-" * 50)

    # Handle empty/filtered data
    if ts_data.empty:
        print("-> Data is empty after preparation. Skipping detection.")
        return {
            "analysis_mode": mode_name,
            "frequency": freq_name,
            "metric_desc": metric_desc,
            "method": method,
            "total_points": 0,
            "anomaly_count": 0,
            "anomalies": [],
            "limit_description": "N/A - Empty Data"
        }

    # 1. Detect
    if method.upper() == 'IQR':
        outliers = detect_iqr_outliers(ts_data, value_col)
        # Determine IQR boundaries
        q1, q3 = ts_data[value_col].quantile(0.25), ts_data[value_col].quantile(0.75)
        iqr = q3 - q1
        lower_limit = q1 - (1.5 * iqr)
        upper_limit = q3 + (1.5 * iqr)
        limit_text = f"IQR Limits: {lower_limit:,.2f} to {upper_limit:,.2f}"
    else:
        outliers = detect_zscore_outliers(ts_data, value_col)
        limit_text = "Z-Score Limit: > +/- 3.0 Standard Deviations"
    num_outliers = len(outliers)
    print(f"-> Total Data Points: {len(ts_data)}")
    print(f"-> **Anomalies Found**: {num_outliers}")
    print(f"-> **Threshold**: {limit_text}")

    report_section = {
        "analysis_mode": mode_name,
        "frequency": freq_name,
        "metric_desc": metric_desc,
        "method": method,
        "limit_description": limit_text,
        "total_points": int(len(ts_data)),
        "anomaly_count": int(num_outliers),
        "anomalies": []
    }

    # ---------------- MANAGE OUTLIER LIST ----------------
    if not outliers.empty:
        print("\n--- ANOMALY DETAILS ---")
        outliers_to_report = outliers.sort_values(by=value_col, ascending=False)
        for _, row in outliers_to_report.iterrows():
            index_id_val = row['index_id']
            # FIXED: Conditional formatting for Timestamp-based indices
            if isinstance(index_id_val, pd.Timestamp):
                if freq_name == 'Monthly':
                    index_id_str = index_id_val.strftime('%Y-%m')  # e.g., 2023-11
                elif freq_name == 'Weekly':
                    index_id_str = index_id_val.strftime('%Y-W%W')  # e.g., 2023-W48
                else:
                    index_id_str = index_id_val.strftime('%Y-%m-%d')  # default
            else:
                index_id_str = str(index_id_val)

            val = row[value_col]

            if method.upper() == 'IQR':
                if row['anomaly_type'] == 'Peak (High)':
                    info = "Breached Upper IQR Limit"
                else:
                    info = "Breached Lower IQR Limit"
            else:
                info = f"Z-Score: {row['z_score']:.2f}"

            # --- IMPROVED OUTPUT FORMAT ---
            print(f"[{index_id_str:<15}] | {row['anomaly_type']:<15} | "
                  f"Value: {val:,.2f} | Reason: {info}")

            report_section['anomalies'].append({
                "index_id": index_id_str,
                "value": float(val),
                "type": row['anomaly_type'],
                "method_details": info
            })
    else:
        print("\n-> **Result:** No anomalies detected based on the chosen threshold.")

    return report_section



# --- 4. The Unified Pipeline Runner (The single callable function - metric_desc update) ---

def perform_anomaly_detection(
    df: pd.DataFrame, 
    value_col: str,
    index_col: str,
    analysis_mode: str,
    metric_desc: str, # Renamed to metric_desc
    frequencies: List[str] = ['D', 'W'],
    method: str = 'IQR',
    output_path: Optional[Union[str, Path]] = None) -> List[Dict]:
    """
    Runs the comprehensive anomaly detection pipeline across different modes.
    """
    full_report = []
    mode = analysis_mode.upper()
    mode_name = get_mode_name(mode)
    
    print("="*80)
    print(f"*** Anomaly Detection Analysis ***")

    # --- Mode Management ---
    if mode == 'TIME_AGGREGATED':
        if not frequencies:
             raise ValueError("frequencies list cannot be empty for TIME_AGGREGATED mode.")
             
        for freq in frequencies:
            ts_data = prepare_data_for_detection(df, value_col, index_col, mode, freq)
            report = anomaly_detection_core(ts_data, method, mode_name, get_freq_name(freq), metric_desc) 
            full_report.append(report)
            
    elif mode in ['TIME_RAW', 'DISTRIBUTIONAL']:
        ts_data = prepare_data_for_detection(df, value_col, index_col, mode)
        report = anomaly_detection_core(ts_data, method, mode_name, get_freq_name(None), metric_desc) 
        full_report.append(report)
        
    else:
        raise ValueError(f"Invalid analysis_mode specified: '{analysis_mode}'. Must be TIME_AGGREGATED, TIME_RAW, or DISTRIBUTIONAL.")
    
    print("\n" + "="*80)
    
    # Robust Saving Logic (metric_desc update)
    if output_path:
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                final_output = {
                    "pipeline_run_details": {
                        "metric_desc": metric_desc, 
                        "method": method,
                        "analysis_mode": mode_name,
                        "checks_run": [f"{r['frequency']} ({r['analysis_mode']})" for r in full_report]
                    },
                    "anomaly_checks": full_report
                }
                json.dump(final_output, f, indent=4)
            print(f"[Anomalies] Successfully saved report to -> {path}")
        except Exception as e:
            print(f"[ERROR] Could not save anomaly report to {output_path}. Error: {e}")
            
    return full_report




def run_anomaly_detection():
    # IMPORT DATA & RUN ANOMALY DETECTION 

    PROJECT_ROOT = Path(__file__).resolve().parents[2] / "python" / "output" /"Anomaly_Detection"

    ###### Sales/Revenue Anomaly Detection for Successful Orders

    df1 = fetch_data_from_bq(q.GET_completed_daily_orders)  # only successful orders ('delivered', 'approved', 'shipped') (agg daily)

    perform_anomaly_detection(df=df1, value_col= 'total_daily_revenue', index_col= 'order_purchase_date',
                            analysis_mode= 'TIME_AGGREGATED',
                            metric_desc= 'Total Sales', frequencies= ['D', 'W'], method= 'IQR',
                            output_path= PROJECT_ROOT / "sales.json")

    ###### Anomaly Detection for Successful Orders

    perform_anomaly_detection(df=df1, value_col= 'total_daily_orders', index_col= 'order_purchase_date',
                            analysis_mode= 'TIME_AGGREGATED',
                            metric_desc= 'Total Successful Orders', frequencies= ['D', 'W'], method= 'IQR',
                            output_path= PROJECT_ROOT / "successful_orders.json")


    ###### Anomaly Detection for Canceled Orders

    df2 = fetch_data_from_bq(q.GET_canceled_daily_orders)  # only canceled orders

    perform_anomaly_detection(df=df2, value_col= 'total_daily_orders', index_col= 'order_purchase_date',
                            analysis_mode= 'TIME_AGGREGATED',
                            metric_desc= 'Total Order Cancellations', frequencies= ['D', 'W'], method= 'IQR',
                            output_path= PROJECT_ROOT / "order_cancellations.json")


    ###### Anomaly Detection for delivery times (number of days between purchase and delivery)

    df3 = fetch_data_from_bq(q.GET_delivery_duration_time_series)  # delivery duration with time (only delivered orders)


    perform_anomaly_detection(df=df3, value_col= 'days_to_delivery', index_col= 'order_purchase_date',
                            analysis_mode= 'DISTRIBUTIONAL',
                            metric_desc= 'Delivery Duration in days', frequencies= ['D', 'W'], method= 'Z-Score',
                            output_path= PROJECT_ROOT / "delivery_duration.json")



if __name__ == "__main__":
    run_anomaly_detection()