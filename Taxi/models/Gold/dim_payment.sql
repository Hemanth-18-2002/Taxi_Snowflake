{{ config(materialized='table') }}

SELECT DISTINCT
    {{ generate_surrogate_key(['payment_type']) }} AS payment_key,
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'UPI'
        WHEN 4 THEN 'Voided Trip'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Dispute'
        ELSE 'Other'
    END AS payment_type_desc
FROM {{ ref('silver_taxi_trips') }}
