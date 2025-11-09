import os
import requests
import pandas as pd
from src.config import FHIR_BASE_URL, PATIENT_BRONZE_PATH
from src.utils.helpers import ensure_dir, get_ts



def fetch_patients(limit=50):
    """
    Fetch patient FHIR resources and store locally as Parquet.
    """
    url = f"{FHIR_BASE_URL}/Patient?_count={limit}"
    
    print(f"Fetching: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API failed: {response.status_code}")

    data = response.json()

    # Extract entries
    entries = data.get("entry", [])
    patient_rows = []

    for e in entries:
        resource = e.get("resource", {})
        patient_rows.append({
            "id": resource.get("id"),
            "gender": resource.get("gender"),
            "birthDate": resource.get("birthDate"),
            "active": resource.get("active")
        })

    df = pd.DataFrame(patient_rows)

    # Output path
    ensure_dir(PATIENT_BRONZE_PATH)
    file_name = f"patient_{get_ts()}.parquet"
    output_path = os.path.join(PATIENT_BRONZE_PATH, file_name)

    df.to_parquet(output_path, index=False)

    print(f"✅ Stored -> {output_path}")


if __name__ == "__main__":
    fetch_patients(limit=50)
