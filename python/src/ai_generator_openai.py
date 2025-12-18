from pathlib import Path
from openai import OpenAI
import os


### DEFINING THE OUTPUT DIRECTORY
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
# LLM call
# -------------------------
def generate_ai_response(
    prompt,
    model="gpt-4o-mini",
    temperature=0.3,
    max_tokens=1200
):
    """
    Sends the full business context + instructions to the LLM
    and returns a single QC / evaluation response.
    """
    client = OpenAI()  # reads OPENAI_API_KEY from env

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=temperature,
        max_tokens=max_tokens
    )

    return response.choices[0].message.content

# -------------------------
# Main pipeline
# -------------------------
def main():
    # Input: consolidated BI + instructions
    input_path = directory / "business_context.txt"
    output_path = directory / "AI_Report_OpenAI.txt"

    # 1. Read full business context
    business_context = read_file(input_path)

    # 2. Send to LLM
    qc_report = generate_ai_response(business_context)

    # 3. Write single consolidated output
    write_file(output_path, qc_report)

    print("✅ AI QC report generated:", output_path)

if __name__ == "__main__":
    main()
