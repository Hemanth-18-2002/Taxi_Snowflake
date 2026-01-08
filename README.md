🚕 NYC Yellow Taxi Analytics Platform
📊 End-to-End Data Engineering & Analytics Case Study

Snowflake | dbt | Streams & Tasks | Streamlit

🌍 Project Overview

The NYC Yellow Taxi dataset captures millions of daily taxi trips across New York City, including pickup/drop-off times, locations, fares, tips, payment methods, and passenger details.

This project builds a cloud-native, production-grade analytics platform to ingest, clean, transform, and analyze taxi trip data using Snowflake, dbt, and Streamlit.
The final outcome is a fully automated data pipeline with interactive dashboards delivering operational and business insights.

🧩 Problem Statement

Raw taxi data is large, messy, and arrives incrementally

Analytical queries on raw data are slow and unreliable

Business users need clean metrics, historical accuracy, and interactive dashboards

Goals

Build a scalable medallion architecture

Ensure data quality & deduplication

Handle late-arriving data

Implement SCD Type-2 dimensions

Deliver real-time-ready analytics dashboards

🛠 Tech Stack

❄ Snowflake

Snowpipe

Streams & Tasks

Time Travel

🧱 dbt (Core / Cloud)

Incremental models

SCD Type-2

Data quality tests

🐍 SQL & Snowpark

📊 Streamlit

Snowflake Native Streamlit

Streamlit Community Cloud (Web)

📦 Altair

☁ Cloud Storage (Parquet files)

📂 Dataset

NYC Yellow Taxi Trip Records

Vendor information

Pickup & drop-off timestamps

Locations

Trip distance

Fare, tips, surcharges

Payment types

Passenger count

Dataset is ingested in Parquet format to ensure performance and schema consistency.

🎯 Key Business Objectives

📈 Track trip volume and revenue trends

⏱ Analyze trip duration & peak hours

💳 Understand payment behavior

👥 Study passenger behavior

💰 Identify high-value trips

📏 Analyze distance vs revenue relationship

🧾 Ensure historical accuracy using SCD-2

🧱 Medallion Architecture
🥉 Bronze / RAW Layer

Raw taxi data ingested as-is

Stored in Snowflake RAW tables

No transformations

Source of truth

Ingestion via:

Snowpipe

COPY INTO

🥈 Silver / CLEANED Layer

Built using dbt incremental models

Key transformations:

Deduplication

Timestamp parsing

Null handling (forward & backward fill)

Business rule validation

Late-arriving data handled with rolling windows

Enforced with dbt tests

🥇 Gold / CURATED Layer

Analytics-ready star schema

Fact & Dimension tables

Optimized for BI & dashboards

📐 Data Model (Star Schema)
Fact Table

FACT_TAXI_TRIPS

One row per taxi trip

Measures:

total_amount

trip_distance

passenger_count

tip_amount

trip_duration

Dimensions

DIM_VENDOR

DIM_LOCATION (SCD Type-2)

DIM_PAYMENT

🔄 Slowly Changing Dimensions (SCD-2)

Implemented on Location Dimension

Tracks historical changes using:

effective_start_date

effective_end_date

is_current

Fact table joins dimensions using:

Business key

Event timestamp (pickup time)

✅ Ensures historical reporting accuracy

🔄 Orchestration & Automation
Automation Flow

Snowpipe

Auto-ingests new files

Streams

Track incremental changes

Tasks

Trigger Silver & Gold transformations

dbt

Incremental models

SCD-2 logic

Data tests

Pipeline is event-driven & cost-efficient.

📊 Streamlit Dashboards
Dashboards Included
📌 Executive KPIs

Total Trips

Total Revenue

Average Fare

Average Trip Duration

📈 Trends & Patterns

Trips & Revenue over time

Trips by pickup hour

Revenue vs distance

👥 Customer Behavior

Passenger count distribution

Tip analysis

Payment type adoption

💼 Operational Insights

Vendor performance

High-value trips (outliers)

Trip duration distribution

Dashboards are available in:

Snowflake Native Streamlit (internal analytics)

Streamlit Web (Cloud) for portfolio & demos

🚀 How to Get Started

Clone the repository

Set up Snowflake database & schema

Configure Snowpipe & stages

Run dbt models:

dbt run
dbt test


Launch Streamlit app:

streamlit run app.py


Explore dashboards

🧠 Key Learnings & Highlights

Built a production-grade medallion architecture

Implemented SCD-2 correctly with fact awareness

Designed incremental & late-data safe pipelines

Automated ingestion with Streams & Tasks

Delivered business-focused dashboards

Deployed both internal & public Streamlit apps
