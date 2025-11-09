# src/ingestion/ingest_bronze.py
import os
import re
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load DB credentials from .env
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

# SQLAlchemy engine
DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI)

# --- Helper functions ---
def clean_table_name(filename: str) -> str:
    """Generate a clean table name from the Parquet file name."""
    base = os.path.splitext(filename)[0]
    # Remove trailing timestamp if exists
    clean_name = re.sub(r'_\d{8,}$', '', base)
    return f"bronze_{clean_name.lower()}"

def recursive_convert(obj):
    """Recursively convert ndarray → list and ensure JSON serializable."""
    if isinstance(obj, np.ndarray):
        return recursive_convert(obj.tolist())
    elif isinstance(obj, list):
        return [recursive_convert(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: recursive_convert(v) for k, v in obj.items()}
    else:
        return obj

def convert_complex_columns_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """Serialize columns with nested structures to JSON strings."""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict, np.ndarray))).any():
            df[col] = df[col].apply(lambda x: json.dumps(recursive_convert(x)) if x is not None else None)
    return df

# --- Main ingestion function ---
def ingest_parquet_to_postgres():
    if not os.path.exists(BRONZE_PATH):
        print(f"No Bronze folder found at {BRONZE_PATH}")
        return

    parquet_files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".parquet")]
    if not parquet_files:
        print("No Parquet files found in Bronze folder.")
        return

    for file in parquet_files:
        file_path = os.path.join(BRONZE_PATH, file)
        table_name = clean_table_name(file)
        print(f"📥 Ingesting {file} → {table_name}...")

        try:
            df = pd.read_parquet(file_path)
            df = convert_complex_columns_to_json(df)

            df.to_sql(
                table_name,
                con=engine,
                if_exists="replace",
                index=False,
                method="multi"
            )
            print(f" {table_name} ingested successfully!")

        except Exception as e:
            print(f" Failed to write {table_name} to Postgres: {e}")

# --- Run script ---
if __name__ == "__main__":
    ingest_parquet_to_postgres()
