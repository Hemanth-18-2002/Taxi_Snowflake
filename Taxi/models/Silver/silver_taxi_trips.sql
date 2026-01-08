{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'VendorID || pickup_ts || PULocationID',
    on_schema_change = 'sync_all_columns'
) }}

WITH source_data AS (

    SELECT
        VendorID,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee,
        cbd_congestion_fee,

        /* Pickup timestamp */
        TO_TIMESTAMP_NTZ(
            tpep_pickup_date || ' ' || tpep_pickup_time,
            'YYYY-MM-DD HH24:MI:SS'
        ) AS pickup_ts,

        /* Dropoff timestamp */
        TO_TIMESTAMP_NTZ(
            tpep_dropoff_date || ' ' || tpep_dropoff_time,
            'YYYY-MM-DD HH24:MI:SS'
        ) AS dropoff_ts

    FROM {{ source('taxi_raw', 'RAW_TAXI_TRIPS') }}

    {% if is_incremental() %}
      WHERE tpep_pickup_date >= (
          SELECT DATEADD(day, -3, MAX(DATE(pickup_ts)))
          FROM {{ this }}
      )
    {% endif %}
),

/* ---------------------------
   1️⃣ REMOVE DUPLICATES
---------------------------- */
deduped AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY VendorID, pickup_ts, PULocationID
                ORDER BY dropoff_ts DESC
            ) AS rn
        FROM source_data
    )
    WHERE rn = 1
),

/* ---------------------------
   2️⃣ FILL NULL VALUES
---------------------------- */
filled AS (

    SELECT
        VendorID,

        /* Passenger count */
        COALESCE(
            passenger_count,
            LAG(passenger_count) IGNORE NULLS OVER w,
            LEAD(passenger_count) IGNORE NULLS OVER w
        ) AS passenger_count,

        /* Trip distance */
        COALESCE(
            trip_distance,
            LAG(trip_distance) IGNORE NULLS OVER w,
            LEAD(trip_distance) IGNORE NULLS OVER w
        ) AS trip_distance,

        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,

        /* Fare amount */
        COALESCE(
            fare_amount,
            LAG(fare_amount) IGNORE NULLS OVER w,
            LEAD(fare_amount) IGNORE NULLS OVER w
        ) AS fare_amount,

        extra,
        mta_tax,

        /* Tip amount */
        COALESCE(
            tip_amount,
            LAG(tip_amount) IGNORE NULLS OVER w,
            LEAD(tip_amount) IGNORE NULLS OVER w
        ) AS tip_amount,

        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee,
        cbd_congestion_fee,

        pickup_ts,
        dropoff_ts

    FROM deduped
    WINDOW w AS (
        PARTITION BY VendorID, PULocationID
        ORDER BY pickup_ts
    )
),

/* ---------------------------
   3️⃣ FINAL DATA QUALITY FILTER
---------------------------- */
cleaned_data AS (

    SELECT *
    FROM filled
    WHERE
        VendorID IS NOT NULL
        AND pickup_ts IS NOT NULL
        AND dropoff_ts IS NOT NULL
        AND trip_distance > 0
        AND fare_amount >= 0
        AND dropoff_ts >= pickup_ts
)

SELECT * FROM cleaned_data
