{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT DATE(pickup_ts) AS date_day
    FROM {{ ref('silver_taxi_trips') }}
)

SELECT
    date_day                                   AS date_key,
    YEAR(date_day)                             AS year,
    MONTH(date_day)                            AS month,
    DAY(date_day)                              AS day,
    DAYOFWEEK(date_day)                        AS day_of_week,
    WEEKOFYEAR(date_day)                       AS week_of_year,
    TO_VARCHAR(date_day, 'MON')                AS month_name,
    CASE WHEN day_of_week IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates
