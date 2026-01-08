SELECT
    LocationID,
    COUNT(*) AS current_versions
FROM TAXI.TAXI.DIM_LOCATION
WHERE is_current = TRUE
GROUP BY LocationID
HAVING COUNT(*) > 1;

SELECT
    LocationID,
    location_key,
    effective_start_date,
    effective_end_date,
    is_current
FROM TAXI.TAXI.DIM_LOCATION
WHERE LocationID = 117
ORDER BY effective_start_date;

SELECT COUNT(*)
FROM TAXI.TAXI.FACT_TAXI_TRIPS
WHERE pickup_location_key IS NULL
   OR dropoff_location_key IS NULL;


SELECT
    f.trip_id,
    f.pickup_ts,
    d.LocationID,
    d.effective_start_date,
    d.effective_end_date
FROM TAXI.TAXI.FACT_TAXI_TRIPS f
JOIN TAXI.TAXI.DIM_LOCATION d
  ON f.pickup_location_key = d.location_key
WHERE f.pickup_ts NOT BETWEEN
      d.effective_start_date AND d.effective_end_date;


SELECT DISTINCT LocationID
FROM TAXI.GOLD.DIM_LOCATION
LIMIT 1;
