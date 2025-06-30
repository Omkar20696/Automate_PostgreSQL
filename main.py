import pandas as pd
from sqlalchemy import create_engine

def load_all_sheets(sample_datasets):
    
    all_sheets = pd.read_excel(sample_datasets, sheet_name=None)
    return all_sheets

filepath = "sample_datasets.xlsx"
all_data = load_all_sheets(filepath)

for sheet_name, df in all_data.items():
    print(f"sheet: {sheet_name}")
    print(df.head())
    print("\n")