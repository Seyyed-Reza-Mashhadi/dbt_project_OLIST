# Fetch data from BigQuery based on provided SQL query
import os
from config.gc_credentials import SERVICE_ACCOUNT_PATH
from google.cloud import bigquery
from google.cloud import bigquery_storage

# Function to fetch data from BigQuery (we can still improve it further, searching for local files before querying BQ)

# --- Global client cache ---
_bq_client = None
_bq_storage_client = None

def get_bq_client():

    global _bq_client, _bq_storage_client
    
    if _bq_client is None or _bq_storage_client is None:
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
            _bq_client = bigquery.Client()
            _bq_storage_client = bigquery_storage.BigQueryReadClient()
            print("BigQuery clients initialized.")
        except Exception as e:
            print(f"FATAL ERROR: Could not initialize BigQuery clients.")
            print(f"Check SERVICE_ACCOUNT_PATH in config.py and ensure the JSON file exists.")
            print(f"Error details: {e}")
            return None, None
    return _bq_client, _bq_storage_client

def fetch_data_from_bq(sql_query):
    client, _ = get_bq_client()
    if client is None:
        return None
    
    try:
        query_job = client.query(sql_query)
        df = query_job.to_dataframe()
        mb_processed = query_job.total_bytes_processed / (1024**2)
        print(f"Query successful. Scanned {mb_processed:.3f} MB.")
        return df
    except Exception as e:
        print(f"--- BIGQUERY QUERY FAILED ---")
        print(f"Error: {e}")
        print(f"Failing Query Snippet:\n{sql_query[:100]}...")
        return None
