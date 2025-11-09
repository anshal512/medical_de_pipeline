import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
import os

# -----------------------------
# Database connection
# -----------------------------
DB_USER = os.getenv("POSTGRES_USER")  # replace if needed
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")  # replace if needed
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------
# Utility to load table safely
# -----------------------------
@st.cache_data
def load_table(table_name):
    try:
        query = f"SELECT * FROM {table_name} LIMIT 1000"
        df = pd.read_sql(query, engine)
        return df
    except (ProgrammingError, OperationalError) as e:
        st.warning(f"⚠️ Table `{table_name}` not found or cannot connect: {e}")
        return pd.DataFrame()  # Return empty DataFrame if table missing

# -----------------------------
# Load dimension tables
# -----------------------------
dim_patient = load_table("dim_patient")
dim_practitioner = load_table("dim_practitioner")
dim_condition = load_table("dim_condition")
dim_observation = load_table("dim_observation")

# -----------------------------
# Load fact tables
# -----------------------------
fact_condition = load_table("fact_condition")
fact_observation = load_table("fact_observation")  # Will safely return empty if missing

# -----------------------------
# Dashboard Header
# -----------------------------
st.set_page_config(page_title="🏥 Medical Dashboard", layout="wide")
st.title("🏥 Medical Data Pipeline Dashboard")
st.markdown("Welcome! Here's an overview of the current medical data pipeline and KPIs.")

# -----------------------------
# KPI Tiles
# -----------------------------
st.header("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)
col1.metric("🧑 Patients", len(dim_patient) if not dim_patient.empty else 0)
col2.metric("👨‍⚕️ Practitioners", len(dim_practitioner) if not dim_practitioner.empty else 0)
col3.metric("📋 Conditions Recorded", len(fact_condition) if not fact_condition.empty else 0)
col4.metric("🔬 Observations Recorded", len(fact_observation) if not fact_observation.empty else 0)

# -----------------------------
# Dimension Tables Overview
# -----------------------------
st.header("📁 Dimension Tables")
with st.expander("View Dimension Tables"):
    if not dim_patient.empty:
        st.subheader("🧑 Patients")
        st.dataframe(dim_patient)
    if not dim_practitioner.empty:
        st.subheader("👨‍⚕️ Practitioners")
        st.dataframe(dim_practitioner)
    if not dim_condition.empty:
        st.subheader("📋 Conditions")
        st.dataframe(dim_condition)
    if not dim_observation.empty:
        st.subheader("🔬 Observations")
        st.dataframe(dim_observation)

# -----------------------------
# Fact Tables Overview
# -----------------------------
st.header("📊 Fact Tables")
with st.expander("View Fact Tables"):
    if not fact_condition.empty:
        st.subheader("📋 Fact Condition")
        st.dataframe(fact_condition)
    if not fact_observation.empty:
        st.subheader("🔬 Fact Observation")
        st.dataframe(fact_observation)
    else:
        st.info("Fact Observation table is not available.")
