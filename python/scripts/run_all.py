import os
from pathlib import Path
from dotenv import load_dotenv

# Importing the .py files
from src.raw_data_qc import run_raw_data_qc            # Step 1
from src.anomaly_detection import run_anomaly_detection    # Step 2a
from src.analysis import run_analysis              # Step 2b
from src.context_builder import run_context_builder   # Step 3
from src.ai_generator import run_ai_generator  # Step 4

# Load environment variables (API Keys, BQ Path)
load_dotenv()

def main():
    print("🚀 --- STARTING OLIST AI-ANALYTICS PIPELINE --- 🚀")
    print("="*50)

    # STEP 1: RAW DATA QC
    print("\n🔍 STEP 1: Running Raw Data Quality Control...")
    try:
        run_raw_data_qc()
        print("✅ Data QC Complete.")
    except Exception as e:
        print(f"❌ QC Failed: {e}")

    # STEP 2: ANOMALY DETECTION
    print("\n📈 STEP 2: Detecting Anomalies in BigQuery Data...")
    try:
        run_anomaly_detection()
        print("✅ Anomaly Detection Complete.")
    except Exception as e:
        print(f"❌ Anomaly Detection Failed: {e}")

    # STEP 3: CORE ANALYSIS (Metrics, KPIs)
    print("\n📊 STEP 3: Computing Core Business Metrics...")
    try:
        run_analysis()
        print("✅ Business Analysis Complete.")
    except Exception as e:
        print(f"❌ Analysis Failed: {e}")

    # STEP 4: CONTEXT BUILDERs
    print("\n📝 STEP 4: Building AI Context from JSON outputs...")
    try:
        run_context_builder()
        print("✅ AI Context built (business_context.txt created).")
    except Exception as e:
        print(f"❌ Context Builder Failed: {e}")

    # STEP 5: AI GENERATOR (Gemini / OpenAI)
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