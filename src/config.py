from pathlib import Path

FHIR_BASE_URL = "http://hapi.fhir.org/baseR4"

BASE_DATA_DIR = Path("data")
BRONZE_DIR = BASE_DATA_DIR / "bronze"

PATIENT_BRONZE_PATH = BRONZE_DIR / "patient"
CONDITION_BRONZE_PATH = BRONZE_DIR / "condition"
PRACTITIONER_BRONZE_PATH = BRONZE_DIR / "practitioner"

PATIENT_BRONZE_PATH.mkdir(parents=True, exist_ok=True)
CONDITION_BRONZE_PATH.mkdir(parents=True, exist_ok=True)
PRACTITIONER_BRONZE_PATH.mkdir(parents=True, exist_ok=True)
