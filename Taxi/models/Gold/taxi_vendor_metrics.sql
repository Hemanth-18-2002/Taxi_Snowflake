{{ config(materialized = 'table') }}

SELECT
    VendorID,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_fare
FROM {{ ref('silver_taxi_trips') }}
GROUP BY VendorID
