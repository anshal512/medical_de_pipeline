# Medical Data Engineering Pipeline

A **weekend project** demonstrating a full medical data engineering workflow—from raw data ingestion to analytics-ready models and interactive dashboards.

---

## Project Overview

This project showcases how to:

- Collect and ingest raw healthcare data (patients, practitioners, conditions, observations)
- Transform and clean data into structured silver tables
- Model a star schema for reporting and analytics
- Build interactive dashboards using Streamlit for actionable insights

---

## Workflow

### Data Sourcing
- Raw healthcare data (patients, practitioners, conditions, observations) is collected from source systems (JSON/CSV).

### Bronze Layer (Raw Ingestion)
- Data is ingested into PostgreSQL as raw bronze tables.
- Minimal transformations applied to preserve original structure.

### Silver Layer (Cleansed & Transformed)
- Raw data is cleaned and normalized using **Pandas**.
- Extracts key fields for analytics.
- Silver tables are created for each entity (patient, practitioner, condition, observation).

### Modeling & Star Schema
- Silver tables are used to build a **star schema**:
  - **Dimension tables:** `dim_patient`, `dim_practitioner`, `dim_condition`, `dim_observation`
  - **Fact tables:** `fact_condition` (aggregated metrics)
- Supports reporting and analytics efficiently.

### Reporting & Visualization
- Interactive dashboards created with **Streamlit**.
- Charts include histograms, bar graphs, pie charts, and KPIs.
- Provides actionable insights from the curated healthcare data.

### Optional Future Enhancements
- Stream data from sources continuously.
- Integrate **Airflow** for orchestration.
- Expand fact tables for additional analytics use cases.

---

## Tech Stack

- **PostgreSQL**: Relational database for bronze & silver tables  
- **Pandas**: Data cleaning & transformation  
- **Streamlit**: Interactive dashboards  
- **Python**: Orchestration & ETL scripts  

---
