import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# -----------------------------
# Database connection
# -----------------------------
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------
# Load tables
# -----------------------------
@st.cache_data
def load_table(table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

dim_patient = load_table("dim_patient")
dim_practitioner = load_table("dim_practitioner")
dim_condition = load_table("dim_condition")
fact_condition = load_table("fact_condition")

# -----------------------------
# Dashboard Title
# -----------------------------
st.set_page_config(page_title="🏥 Medical Dashboard", layout="wide")
st.title("🏥 Medical Data Analytics Dashboard")
st.markdown("Analyze patient, practitioner, and condition metrics interactively.")

# -----------------------------
# KPI Tiles
# -----------------------------
st.subheader("📊 Key Metrics")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(label="Total Patients", value=dim_patient.shape[0], delta=f"{fact_condition['subject_reference'].nunique()} unique conditions")
with col2:
    st.metric(label="Total Practitioners", value=dim_practitioner.shape[0], delta=f"{fact_condition['asserter_reference'].nunique()} active")
with col3:
    st.metric(label="Total Conditions", value=fact_condition.shape[0], delta=f"{dim_condition.shape[0]} in dimension table")

# -----------------------------
# Charts Section
# -----------------------------
st.markdown("---")
st.subheader("📈 Conditions Analysis")

# Two columns for charts
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🩺 Conditions per Practitioner")
    chart_df = fact_condition.groupby("asserter_reference").size().reset_index(name="count")
    fig_bar = px.bar(
        chart_df,
        x="asserter_reference",
        y="count",
        color="count",
        text="count",
        title="Conditions by Practitioner",
        labels={"asserter_reference": "Practitioner", "count": "Number of Conditions"},
        color_continuous_scale="Viridis"
    )
    fig_bar.update_layout(paper_bgcolor="rgba(240,240,240,0.5)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.markdown("### 🧾 Conditions per Patient")
    fig_hist = px.histogram(
        fact_condition,
        x="subject_reference",
        nbins=10,
        title="Conditions Count per Patient",
        labels={"subject_reference": "Patient", "count": "Number of Conditions"},
        color_discrete_sequence=["#636EFA"]
    )
    fig_hist.update_layout(paper_bgcolor="rgba(240,240,240,0.5)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_hist, use_container_width=True)

# -----------------------------
# Pie Chart
# -----------------------------
st.markdown("---")
st.subheader("📊 Conditions Distribution")
fig_pie = px.pie(
    fact_condition,
    names="subject_reference",
    title="Proportion of Conditions per Patient",
    hole=0.3
)
fig_pie.update_layout(
    margin=dict(t=50, b=50, l=50, r=50),
    legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    paper_bgcolor="rgba(240,240,240,0.5)"
)
st.plotly_chart(fig_pie, use_container_width=True)

# -----------------------------
# Expanders for Raw Data
# -----------------------------
with st.expander("Show Fact Condition Table"):
    st.dataframe(fact_condition)

with st.expander("Show Dim Tables"):
    st.markdown("**Patients**")
    st.dataframe(dim_patient)
    st.markdown("**Practitioners**")
    st.dataframe(dim_practitioner)
    st.markdown("**Conditions**")
    st.dataframe(dim_condition)
