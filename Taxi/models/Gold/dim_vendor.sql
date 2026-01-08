{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['VendorID']) }} AS vendor_key,
    VendorID AS vendor_id,
    CASE VendorID
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone'
        ELSE 'Unknown'
    END AS vendor_name
FROM {{ ref('silver_taxi_trips') }}
GROUP BY VendorID
