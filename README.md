# 🚕 NYC Yellow Taxi Analytics Platform
### 📊 End-to-End Data Engineering & Analytics Case Study  
**Snowflake | dbt | Streams & Tasks | Streamlit**

---

## 🌍 Project Overview

The **NYC Yellow Taxi dataset** captures millions of taxi trips across New York City, including pickup/drop-off times, locations, fares, tips, passenger counts, and payment methods.

This project implements a **production-grade, cloud-native analytics platform** that ingests raw taxi data, cleans and transforms it using modern data engineering practices, and delivers **interactive dashboards** for business insights.

The solution closely mirrors **real-world enterprise data platforms** used in analytics and reporting teams.

---

## 🧩 Problem Statement

Raw taxi data is:
- High-volume and continuously arriving
- Messy and inconsistent
- Not analytics-ready

### 🎯 Objectives
- Build a scalable **medallion architecture**
- Handle **incremental & late-arriving data**
- Ensure **data quality and deduplication**
- Implement **SCD Type-2 dimensions**
- Deliver **interactive dashboards** for insights

---

## 🛠 Tech Stack

- ❄ **Snowflake**
  - Snowpipe
  - Streams & Tasks
  - Time Travel
- 🧱 **dbt (Core / Cloud)**
  - Incremental models
  - SCD Type-2
  - Data quality tests
- 🐍 **SQL & Snowpark**
- 📊 **Streamlit**
  - Snowflake Native Streamlit
  - Streamlit Community Cloud
- 📈 **Altair**
- ☁ **Parquet-based ingestion**

---

## 📂 Dataset

**NYC Yellow Taxi Trip Records**

Includes:
- Vendor information  
- Pickup & drop-off timestamps  
- Pickup & drop-off locations  
- Trip distance  
- Fare, tips & surcharges  
- Passenger count  
- Payment types  

Data is ingested in **Parquet format** for performance and schema consistency.

---

## 🧱 Medallion Architecture

<img width="700" height="516" alt="image" src="https://github.com/user-attachments/assets/9ae83ed4-27df-4c2c-a4dd-25ac47045561" />

### 🥉 Bronze / RAW Layer
- Stores raw taxi data as-is
- Loaded using **Snowpipe / COPY INTO**
- Acts as the immutable source of truth

---

### 🥈 Silver / CLEANED Layer
- Built using **dbt incremental models**
- Key transformations:
  - Deduplication
  - Timestamp parsing
  - Null handling (forward & backward fill)
  - Business rule validation
- Late-arriving data handled safely
- Enforced with **dbt tests**

---

### 🥇 Gold / CURATED Layer
- Analytics-ready **star schema**
- Optimized for BI & dashboards
- Fact & dimension tables

---

## 📐 Data Model (Star Schema)

![Star Schema](images/star_schema.png)

### ⭐ Fact Table
**FACT_TAXI_TRIPS**
- One row per taxi trip
- Measures:
  - `total_amount`
  - `trip_distance`
  - `passenger_count`
  - `tip_amount`
  - `trip_duration`

### 📘 Dimensions
- **DIM_VENDOR**
- **DIM_PAYMENT**
- **DIM_LOCATION** *(SCD Type-2)*

---

## 🔄 Slowly Changing Dimension (SCD-2)

- Implemented on **Location Dimension**
- Tracks historical changes using:
  - `effective_start_date`
  - `effective_end_date`
  - `is_current`
- Fact table joins SCD-2 dimension using:
  - Business key
  - Event timestamp (pickup time)

✅ Ensures **historically accurate reporting**

---

## 🔄 Orchestration & Automation

<img width="400" height="700" alt="image" src="https://github.com/user-attachments/assets/7cd930ec-2d86-4028-a7a7-c3c9d6d8a3dd" />


- **Snowpipe** → Auto-ingestion
- **Streams** → Track incremental changes
- **Tasks** → Trigger Silver & Gold transformations
- **dbt** → Incremental models & tests

Pipeline is **event-driven, automated, and cost-efficient**.

---

## 📊 Streamlit Dashboards

<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/133fb12b-77a9-43af-a1db-734b17daf7d0" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/a21b096d-98a8-4005-a531-4db0143cc3cc" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/8e6439c7-c66d-44db-8067-21209b929c4b" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/466c0775-8a1b-4ecc-ad15-ae149113155d" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/2cb6397d-9958-451c-83f6-4ac3eeb0e994" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/d117aa76-5752-4422-af70-b233b06965de" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/1928509e-ffd9-4459-9080-5eec6818e465" />
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/0fc2fd22-9885-4385-986c-41be60c6f9a6" />


### Dashboards Included

### 📌 Executive KPIs
- Total Trips
- Total Revenue
- Average Fare
- Average Trip Duration

### 📈 Trends & Patterns
- Trips & revenue over time
- Trips by pickup hour
- Revenue vs distance

### 👥 Customer Behavior
- Passenger count distribution
- Tip analysis
- Payment type adoption

### 🚨 Operational Insights
- Vendor performance
- High-value trips
- Trip duration distribution

Dashboards are available in:
- **Snowflake Native Streamlit**
- **Streamlit Community Cloud (Web)**

---

## 🚀 How to Run the Project

```bash
# Run dbt models
dbt run
dbt test

# Launch Streamlit app
streamlit run app.py
