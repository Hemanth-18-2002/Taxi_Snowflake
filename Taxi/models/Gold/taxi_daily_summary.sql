{{ config(materialized = 'table') }}

SELECT
    DATE(pickup_ts) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(total_amount) AS avg_fare
FROM {{ ref('silver_taxi_trips') }}
GROUP BY trip_date
