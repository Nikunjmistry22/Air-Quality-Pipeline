from extraction import extract_data
from loader import load_data

# Step 1: Extract data
try:
    print("Extraction Started")
    df = extract_data()
    print("Data extracted successfully.")
except Exception as e:
    print(f"Error during extraction: {e}")

# Step 2: Load data into Supabase with the new schema
try:
    print("Loading Started")
    load_data(df, schema='public')
    print("Data loaded successfully.")
except Exception as e:
    print(f"Error during loading: {e}")
