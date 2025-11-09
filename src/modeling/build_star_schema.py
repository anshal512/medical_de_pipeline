# src/modeling/build_star_schema.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI)

def load_silver_table(table_name):
    """Load a silver table from Postgres."""
    df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    return df

def build_dim_patient(df):
    cols = ['id', 'name', 'gender', 'birthDate', 'identifier', 'address']
    cols = [c for c in cols if c in df.columns]
    dim_patient = df[cols].copy()
    dim_patient.to_sql('dim_patient', con=engine, if_exists='replace', index=False)
    print(f"✅ dim_patient created ({len(dim_patient)} rows)")
    return dim_patient

def build_dim_practitioner(df):
    cols = ['id', 'identifier', 'name', 'active', 'telecom', 'address', 'gender', 'qualification']
    cols = [c for c in cols if c in df.columns]
    dim_practitioner = df[cols].copy()
    dim_practitioner.to_sql('dim_practitioner', con=engine, if_exists='replace', index=False)
    print(f" dim_practitioner created ({len(dim_practitioner)} rows)")
    return dim_practitioner

def build_dim_condition(df):
    cols = ['id', 'code_coding', 'clinicalStatus_coding', 'severity_coding', 'onsetDateTime', 'abatementDateTime']
    cols = [c for c in cols if c in df.columns]
    dim_condition = df[cols].copy()
    dim_condition.to_sql('dim_condition', con=engine, if_exists='replace', index=False)
    print(f" dim_condition created ({len(dim_condition)} rows)")
    return dim_condition

def build_fact_condition(df):
    # Keep foreign keys only
    cols = ['id', 'subject_reference', 'asserter_reference', 'encounter_reference']
    cols = [c for c in cols if c in df.columns]
    fact_condition = df[cols].copy()
    fact_condition.to_sql('fact_condition', con=engine, if_exists='replace', index=False)
    print(f" fact_condition created ({len(fact_condition)} rows)")
    return fact_condition

def build_dim_observation(df):
    cols = ['id', 'code_coding', 'valueQuantity_value', 'valueQuantity_unit', 'valueQuantity_system', 'valueQuantity_code', 'subject_reference']
    cols = [c for c in cols if c in df.columns]
    dim_observation = df[cols].copy()
    dim_observation.to_sql('dim_observation', con=engine, if_exists='replace', index=False)
    print(f" dim_observation created ({len(dim_observation)} rows)")
    return dim_observation

def main():
    # Load silver tables
    silver_patient = load_silver_table("silver_patient")
    silver_practitioner = load_silver_table("silver_practitioner")
    silver_condition = load_silver_table("silver_condition")
    silver_observation = load_silver_table("silver_observation")

    print(f" Loaded silver_patient ({len(silver_patient)} rows)")
    print(f" Loaded silver_practitioner ({len(silver_practitioner)} rows)")
    print(f" Loaded silver_condition ({len(silver_condition)} rows)")
    print(f" Loaded silver_observation ({len(silver_observation)} rows)")

    # Build dimensions
    build_dim_patient(silver_patient)
    build_dim_practitioner(silver_practitioner)
    build_dim_condition(silver_condition)
    build_dim_observation(silver_observation)

    # Build fact tables
    build_fact_condition(silver_condition)

if __name__ == "__main__":
    main()
