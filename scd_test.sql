SELECT DISTINCT LocationID
FROM TAXI.TAXI.DIM_LOCATION
LIMIT 1;

SELECT
    LocationID,
    location_key,
    effective_start_date,
    effective_end_date,
    is_current
FROM TAXI.TAXI.DIM_LOCATION
WHERE LocationID = 61
ORDER BY effective_start_date;

UPDATE TAXI.TAXI.DIM_LOCATION
SET
    effective_end_date = CURRENT_TIMESTAMP(),
    is_current = FALSE
WHERE LocationID = 61
  AND is_current = TRUE;

INSERT INTO TAXI.TAXI.DIM_LOCATION (
    location_key,
    LocationID,
    effective_start_date,
    effective_end_date,
    is_current,
    record_hash
)
SELECT
    UUID_STRING(),
    61,
    CURRENT_TIMESTAMP(),
    TO_TIMESTAMP_NTZ('9999-12-31'),
    TRUE,
    UUID_STRING();





