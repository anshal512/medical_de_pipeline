# src/transformation/transform_silver.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()

DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypassword")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "medical_db")

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI)

# Mapping of bronze → silver
TABLES_MAPPING = {
    "bronze_patient": "silver_patient",
    "bronze_practitioner": "silver_practitioner",
    "bronze_condition": "silver_condition",
    "bronze_observation": "silver_observation",
}

# Expected columns for transformations (add missing columns if not present)
EXPECTED_COLUMNS = {
    "silver_patient": ["telecom", "address", "identifier", "gender", "birthDate", "deceasedBoolean"],
    "silver_practitioner": ["telecom", "address", "identifier", "active", "gender", "qualification"],
    "silver_condition": ["subject_reference", "code_coding", "clinicalStatus_coding", "code_text", "onsetDateTime"],
    "silver_observation": ["subject_reference", "code_coding", "valueQuantity_value", "valueQuantity_unit", "status"],
}

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Replace dots in column names with underscores for SQL compatibility."""
    df.columns = [c.replace(".", "_") for c in df.columns]
    return df

def add_missing_columns(df: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df

def transform_bronze_to_silver():
    for bronze_table, silver_table in TABLES_MAPPING.items():
        print(f" Transforming {bronze_table} → {silver_table}...")
        try:
            # Load bronze table
            df = pd.read_sql(f"SELECT * FROM {bronze_table}", con=engine)
            df = clean_column_names(df)
            
            # Add missing expected columns
            expected_cols = EXPECTED_COLUMNS.get(silver_table, [])
            df = add_missing_columns(df, expected_cols)
            
            # Write to silver table
            df.to_sql(silver_table, con=engine, if_exists="replace", index=False, method="multi")
            print(f" {silver_table} created successfully! ({len(df)} rows)")
        except Exception as e:
            print(f" Failed to transform {bronze_table} → {silver_table}: {e}")

if __name__ == "__main__":
    transform_bronze_to_silver()
