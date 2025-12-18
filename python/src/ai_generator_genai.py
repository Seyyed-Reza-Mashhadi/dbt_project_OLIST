from pathlib import Path
import os
from google import genai  # Modern Gemini SDK

# --- PATH CONFIGURATION ---
# This assumes your script is in D:\My_Projects\OLIST\python\src
# and your data is in D:\My_Projects\OLIST\python\output
directory = Path(__file__).resolve().parents[2] / "python" / "output"  

def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(file_path, content):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

# -------------------------
# Gemini LLM call
# -------------------------
def generate_gemini_response(prompt, model_name="gemini-2.5-flash"): # Updated to 2.5
    """
    Uses Gemini 2.5 Flash. 
    This is the current stable model with high free-tier limits.
    """
    client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))
    
    response = client.models.generate_content(
        model=model_name,
        contents=prompt
    )
    return response.text

# -------------------------
# Main pipeline
# -------------------------
def main():
    input_path = directory / "business_context.txt"
    output_path = directory / "AI_Report_Gemini.txt"
    
    if not input_path.exists():
        print(f"❌ Error: Could not find {input_path}")
        return

    # 1. Read full business context
    print("📖 Reading business context from txt...")
    business_context = read_file(input_path)

    # 2. Generate Gemini Report
    print(f"✨ Generating Gemini report (gemini-2.0-flash)...")
    try:
        gemini_report = generate_gemini_response(business_context)
        
        # 3. Write output
        write_file(output_path, gemini_report)
        print(f"✅ Success! Report generated here: {output_path}")
        
    except Exception as e:
        print(f"❌ Gemini Error: {e}")

if __name__ == "__main__":
    main()