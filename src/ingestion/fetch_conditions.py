import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from src.config import FHIR_BASE_URL, CONDITION_BRONZE_PATH


def fetch_conditions(batch_size=50):
    url = f"{FHIR_BASE_URL}/Condition?_count={batch_size}"
    print("Fetching:", url)

    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    entries = data.get("entry", [])
    records = []

    for e in entries:
        resource = e.get("resource", {})
        records.append({
            "id": resource.get("id"),
            "recordedDate": resource.get("recordedDate"),
            "subject_reference": resource.get("subject", {}).get("reference"),
            "code_text": (
                resource.get("code", {})
                .get("coding", [{}])[0]
                .get("display")
            )
        })

    df = pd.DataFrame(records)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = CONDITION_BRONZE_PATH / f"condition_{ts}.parquet"

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    print(f"✅ Stored -> {file_path}")


if __name__ == "__main__":
    fetch_conditions()
