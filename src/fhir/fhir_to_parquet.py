# src/ingestion/fhir.py
import requests
import pandas as pd
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
os.makedirs(BRONZE_PATH, exist_ok=True)

FHIR_SERVER = "https://your-fhir-server.com"  # replace with real server

def fetch_fhir_resource(resource_type: str, count: int = 50) -> pd.DataFrame:
    """Fetch FHIR resource from API and convert to DataFrame"""
    url = f"{FHIR_SERVER}/{resource_type}?_count={count}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    entries = data.get("entry", [])
    resources = [entry.get("resource") for entry in entries if entry.get("resource")]
    return pd.json_normalize(resources)  # flatten nested JSON

def save_parquet(df: pd.DataFrame, resource_type: str):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{resource_type}_{timestamp}.parquet"
    file_path = os.path.join(BRONZE_PATH, filename)
    df.to_parquet(file_path, index=False)
    print(f" {filename} saved in Bronze folder.")

if __name__ == "__main__":
    for resource in ["Patient", "Practitioner", "Condition", "Observation"]:
        df = fetch_fhir_resource(resource)
        save_parquet(df, resource.lower())
