from pathlib import Path
import os
from dotenv import load_dotenv
from openai import OpenAI
from google import genai  

# --- PATH CONFIGURATION ---
basedir = Path(__file__).resolve().parents[2]
load_dotenv(basedir / '.env')

# export destination folder
directory = Path(__file__).resolve().parents[2] / "python" / "output"

# -------------------------
# File helpers
# -------------------------
def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(file_path, content):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

# -------------------------
# OpenAI LLM call
# -------------------------
def generate_openai_response(prompt, model="gpt-4o-mini"):
    """
    Sends prompt to OpenAI. Includes error handling for Rate Limits.
    """
    try:
        client = OpenAI() # uses OPENAI_API_KEY from environment
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"⚠️ OpenAI Error: {e}"

# -------------------------
# Gemini LLM call
# -------------------------
def generate_gemini_response(prompt, model_name="gemini-2.5-flash"):
    """
    Uses the new google-genai Client. 
    It automatically detects the 'GOOGLE_API_KEY' environment variable.
    """
    try:
        client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))
        response = client.models.generate_content(
            model=model_name,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"⚠️ Gemini Error: {e}"

# -------------------------
# Main pipeline
# -------------------------
def run_ai_generator():
    input_path = directory / "business_context.txt"
    
    if not input_path.exists():
        print(f"❌ Error: Could not find {input_path}")
        return

    # 1. Read full business context
    print("📖 Reading business context...")
    business_context = read_file(input_path)

    # 2. Generate OpenAI Report
    print("🤖 Generating OpenAI report...")
    openai_report = generate_openai_response(business_context)
    write_file(directory / "AI_Report_OpenAI.txt", openai_report)

    # 3. Generate Gemini Report
    print("✨ Generating Gemini report...")
    gemini_report = generate_gemini_response(business_context)
    write_file(directory / "AI_Report_Gemini.txt", gemini_report)

    print(f"\n✅ Done! Check your output folder:")
    print(f"📍 {directory}")

if __name__ == "__main__":
    run_ai_generator()