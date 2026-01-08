{{ config(materialized = 'table') }}

SELECT
    PULocationID,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM {{ ref('silver_taxi_trips') }}
GROUP BY PULocationID
