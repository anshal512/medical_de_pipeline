import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from src.config import FHIR_BASE_URL, PRACTITIONER_BRONZE_PATH


def fetch_practitioners(batch_size=50):
    url = f"{FHIR_BASE_URL}/Practitioner?_count={batch_size}"
    print("Fetching:", url)

    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    entries = data.get("entry", [])
    records = []

    for e in entries:
        resource = e.get("resource", {})

        names = resource.get("name", [])
        full_name = None
        if names:
            given = names[0].get("given", [""])
            family = names[0].get("family", "")
            full_name = f"{' '.join(given)} {family}".strip()

        records.append({
            "id": resource.get("id"),
            "name": full_name,
            "gender": resource.get("gender"),
            "identifier": (
                resource.get("identifier", [{}])[0]
                .get("value")
            )
        })

    df = pd.DataFrame(records)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = PRACTITIONER_BRONZE_PATH / f"practitioner_{ts}.parquet"

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    print(f"✅ Stored -> {file_path}")


if __name__ == "__main__":
    fetch_practitioners()
