@echo off
:: 1. Ensure we start in the OLIST root (where this .bat is)
cd /d "D:\My_Projects\OLIST"

echo 🛠️  Activating Virtual Environment from OLIST root...
:: 2. Activate from the root folder
IF EXIST ".venv\Scripts\activate" (
    call .venv\Scripts\activate
) ELSE (
    echo ❌ ERROR: Could not find .venv in %cd%
    pause
    exit
)

:: 3. Now move into the python folder to run the code
echo 📂 Moving to python directory...
cd python

echo 🚀 Starting OLIST AI Analytics Pipeline...
:: 4. Run the module
python -m scripts.run_all

echo.
echo ✅ Pipeline Execution Finished!
pause