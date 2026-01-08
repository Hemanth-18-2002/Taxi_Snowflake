import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

# ====================================================
# Snowflake Connection (Web / Streamlit Cloud)
# ====================================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database="TAXI",
        schema="TAXI"
    )

@st.cache_data
def load_df(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# ====================================================
# Streamlit Page Config
# ====================================================
st.set_page_config(
    page_title="NYC Yellow Taxi Analytics",
    layout="wide"
)

st.title("🚕 NYC Yellow Taxi Analytics Dashboard")

# ====================================================
# KPI SECTION
# ====================================================
kpi_df = load_df("""
SELECT
    COUNT(*) AS TOTAL_TRIPS,
    ROUND(SUM(total_amount), 2) AS TOTAL_REVENUE,
    ROUND(AVG(total_amount), 2) AS AVG_FARE,
    ROUND(AVG(DATEDIFF(minute, pickup_ts, dropoff_ts)), 2) AS AVG_TRIP_DURATION
FROM FACT_TAXI_TRIPS
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", f"{int(kpi_df.TOTAL_TRIPS[0]):,}")
col2.metric("Total Revenue ($)", f"{kpi_df.TOTAL_REVENUE[0]:,.2f}")
col3.metric("Avg Fare ($)", f"{kpi_df.AVG_FARE[0]:.2f}")
col4.metric("Avg Trip Duration (min)", f"{kpi_df.AVG_TRIP_DURATION[0]}")

st.divider()

# ====================================================
# Trips & Revenue Over Time
# ====================================================
trend_df = load_df("""
SELECT
    DATE(pickup_ts) AS TRIP_DATE,
    COUNT(*) AS TRIPS,
    SUM(total_amount) AS REVENUE
FROM FACT_TAXI_TRIPS
GROUP BY TRIP_DATE
ORDER BY TRIP_DATE
""")

trend_melted = trend_df.melt(
    id_vars="TRIP_DATE",
    value_vars=["TRIPS", "REVENUE"],
    var_name="METRIC",
    value_name="VALUE"
)

trend_chart = alt.Chart(trend_melted).mark_line(point=True).encode(
    x=alt.X("TRIP_DATE:T", title="Date"),
    y=alt.Y("VALUE:Q", title="Value"),
    color="METRIC:N"
).properties(
    title="Trips & Revenue Over Time",
    height=350
)

st.altair_chart(trend_chart, use_container_width=True)

st.divider()

# ====================================================
# Trips by Pickup Hour
# ====================================================
hour_df = load_df("""
SELECT
    HOUR(pickup_ts) AS PICKUP_HOUR,
    COUNT(*) AS TRIPS
FROM FACT_TAXI_TRIPS
GROUP BY PICKUP_HOUR
ORDER BY PICKUP_HOUR
""")

hour_chart = alt.Chart(hour_df).mark_bar().encode(
    x=alt.X("PICKUP_HOUR:O", title="Pickup Hour"),
    y=alt.Y("TRIPS:Q", title="Trips"),
    tooltip=["TRIPS"]
).properties(
    title="Trips by Pickup Hour",
    height=300
)

st.altair_chart(hour_chart, use_container_width=True)

st.divider()

# ====================================================
# Vendor Performance
# ====================================================
vendor_df = load_df("""
SELECT
    dv.vendor_name AS VENDOR,
    COUNT(*) AS TRIPS,
    ROUND(SUM(f.total_amount), 2) AS REVENUE
FROM FACT_TAXI_TRIPS f
JOIN DIM_VENDOR dv
  ON f.vendor_key = dv.vendor_key
GROUP BY dv.vendor_name
""")

vendor_chart = alt.Chart(vendor_df).mark_bar().encode(
    x=alt.X("VENDOR:N", title="Vendor"),
    y=alt.Y("REVENUE:Q", title="Revenue ($)"),
    tooltip=["TRIPS", "REVENUE"]
).properties(
    title="Vendor Revenue Comparison",
    height=300
)

st.altair_chart(vendor_chart, use_container_width=True)

st.divider()

# ====================================================
# Revenue by Payment Type (using PAYMENT_KEY)
# ====================================================
payment_df = load_df("""
SELECT
    dp.payment_type_desc AS PAYMENT_TYPE,
    COUNT(*) AS TRIPS,
    ROUND(SUM(f.total_amount), 2) AS REVENUE,
    ROUND(AVG(f.total_amount), 2) AS AVG_FARE
FROM FACT_TAXI_TRIPS f
JOIN DIM_PAYMENT dp
  ON f.payment_key = dp.payment_key
GROUP BY dp.payment_type_desc
""")

payment_chart = alt.Chart(payment_df).mark_bar().encode(
    x=alt.X("PAYMENT_TYPE:N", title="Payment Type"),
    y=alt.Y("REVENUE:Q", title="Revenue ($)"),
    tooltip=["TRIPS", "REVENUE", "AVG_FARE"]
).properties(
    title="Revenue by Payment Type",
    height=300
)

st.altair_chart(payment_chart, use_container_width=True)

# ====================================================
# Passenger Behavior
# ====================================================
st.divider()
st.subheader("👥 Passenger Behavior")

passenger_df = load_df("""
SELECT
    passenger_count AS PASSENGERS,
    COUNT(*) AS TRIPS,
    ROUND(AVG(total_amount), 2) AS AVG_REVENUE
FROM FACT_TAXI_TRIPS
GROUP BY passenger_count
ORDER BY passenger_count
""")

passenger_chart = alt.Chart(passenger_df).mark_bar().encode(
    x=alt.X("PASSENGERS:O", title="Passenger Count"),
    y=alt.Y("TRIPS:Q", title="Trips"),
    tooltip=["TRIPS", "AVG_REVENUE"]
).properties(
    height=300
)

st.altair_chart(passenger_chart, use_container_width=True)

# ====================================================
# Revenue vs Distance
# ====================================================
st.divider()
st.subheader("📏 Revenue vs Distance")

distance_df = load_df("""
SELECT
    ROUND(trip_distance, 0) AS DISTANCE_BUCKET,
    COUNT(*) AS TRIPS,
    ROUND(AVG(total_amount), 2) AS AVG_REVENUE
FROM FACT_TAXI_TRIPS
WHERE trip_distance > 0
GROUP BY DISTANCE_BUCKET
ORDER BY DISTANCE_BUCKET
""")

distance_chart = alt.Chart(distance_df).mark_line(point=True).encode(
    x=alt.X("DISTANCE_BUCKET:Q", title="Trip Distance (miles)"),
    y=alt.Y("AVG_REVENUE:Q", title="Avg Revenue"),
    tooltip=["TRIPS", "AVG_REVENUE"]
).properties(
    height=300
)

st.altair_chart(distance_chart, use_container_width=True)

# ====================================================
# Tip Analysis
# ====================================================
st.divider()
st.subheader("💰 Tip Analysis")

tip_df = load_df("""
SELECT
    ROUND(tip_amount, 0) AS TIP_BUCKET,
    COUNT(*) AS TRIPS
FROM FACT_TAXI_TRIPS
WHERE tip_amount >= 0
GROUP BY TIP_BUCKET
ORDER BY TIP_BUCKET
""")

tip_chart = alt.Chart(tip_df).mark_bar().encode(
    x=alt.X("TIP_BUCKET:Q", title="Tip Amount ($)"),
    y=alt.Y("TRIPS:Q", title="Trips"),
    tooltip=["TRIPS"]
).properties(
    height=300
)

st.altair_chart(tip_chart, use_container_width=True)

# ====================================================
# Trip Duration Distribution
# ====================================================
st.divider()
st.subheader("⏱️ Trip Duration Distribution")

duration_df = load_df("""
SELECT
    FLOOR(DATEDIFF(minute, pickup_ts, dropoff_ts) / 5) * 5 AS DURATION_BUCKET,
    COUNT(*) AS TRIPS
FROM FACT_TAXI_TRIPS
WHERE dropoff_ts > pickup_ts
GROUP BY DURATION_BUCKET
ORDER BY DURATION_BUCKET
""")

duration_chart = alt.Chart(duration_df).mark_bar().encode(
    x=alt.X("DURATION_BUCKET:Q", title="Duration (minutes)"),
    y=alt.Y("TRIPS:Q", title="Trips"),
    tooltip=["TRIPS"]
).properties(
    height=300
)

st.altair_chart(duration_chart, use_container_width=True)

# ====================================================
# High Value Trips
# ====================================================
st.divider()
st.subheader("🚨 High-Value Trips")

high_value_df = load_df("""
SELECT
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance,
    total_amount
FROM FACT_TAXI_TRIPS
ORDER BY total_amount DESC
LIMIT 20
""")

st.dataframe(high_value_df, use_container_width=True)
