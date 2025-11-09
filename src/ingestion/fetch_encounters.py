import os
import requests
import pandas as pd

from src.config import FHIR_BASE_URL, BRONZE_PATH
from src.utils.helpers import ensure_dir, get_ts

ENC_PATH = os.path.join(BRONZE_PATH, "encounter")

def fetch_encounters(limit=50):
    url = f"{FHIR_BASE_URL}/Encounter?_count={limit}"
    print(f"Fetching: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API failed: {response.status_code}")

    data = response.json()
    entries = data.get("entry", [])

    rows = []

    for e in entries:
        resource = e.get("resource", {})
        rows.append({
            "id": resource.get("id"),
            "status": resource.get("status"),
            "class": resource.get("class"),
            "subject": resource.get("subject"),
            "type": resource.get("type"),
            "period": resource.get("period")
        })

    df = pd.DataFrame(rows)

    ensure_dir(ENC_PATH)
    file_name = f"encounter_{get_ts()}.parquet"
    file_path = os.path.join(ENC_PATH, file_name)

    df.to_parquet(file_path, index=False)
    print(f"✅ Stored -> {file_path}")


if __name__ == "__main__":
    fetch_encounters()
