# DCWP Complaints — Snowflake Mini Project

## Project Overview
This is a compact, end-to-end data engineering project on **Snowflake** that takes a real CSV dataset of DCWP (consumer complaints) and turns it into analysis-ready tables and an optional interactive app. The flow is simple and production-flavored: data lands in a **RAW** layer via a defined **FILE FORMAT** and **Stage** (S3 or internal), is loaded with **COPY INTO**, then cleaned and standardized into **STAGING** using **Snowflake Dynamic Tables**. A second dynamic table aggregates the data into the **MART** layer (e.g., monthly counts and average restitution by industry and complaint type), providing a stable, query-friendly surface for analytics tools.

The project showcases Snowflake best practices—layered architecture (RAW/STAGING/MART), schema-driven ingestion, idempotent SQL, and fully managed refresh logic through Dynamic Tables (no external schedulers required). To help explore results, it includes a **Streamlit** application that can run **inside Snowflake** or **locally**. The app exposes filters (industry, complaint type), key metrics (row counts, average restitution, distinct industries), and simple trend/Top-N views, plus a CSV download of filtered results.

 It’s intentionally minimal but complete—clear SQL scripts for setup and pipeline, a small Streamlit UI for consumption, and a structure that scales from a demo to a lightweight production pattern.


