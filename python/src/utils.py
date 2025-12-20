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
            # 1. Dynamically locate .env in project root (2 levels up from src/)
            basedir = Path(__file__).resolve().parents[2]
            load_dotenv(basedir / '.env')
            
            # 2. Initialize Clients
            # These automatically look for GOOGLE_APPLICATION_CREDENTIALS in .env
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
        # Run the query job
        query_job = client.query(sql_query)
        
        # Download the results using the storage_client (Fast Path)
        df = query_job.to_dataframe(bqstorage_client=storage_client)
        
        # Calculate costs/usage for visibility
        mb_processed = query_job.total_bytes_processed / (1024**2)
        print(f"✔️ Query successful. Scanned {mb_processed:.2f} MB. Loaded {len(df)} rows.")
        
        return df
        
    except Exception as e:
        print("\n--- ⚠️ BIGQUERY QUERY FAILED ---")
        print(f"Error: {e}")
        print(f"Check your SQL syntax in sql_queries.py.")
        # Print the first 100 characters of the failing query to help debug
        print(f"Failing Query Snippet: {sql_query.strip()[:100]}...\n")
        return None