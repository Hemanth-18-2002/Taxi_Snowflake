{{ config(
    materialized = 'incremental',
    unique_key = 'VendorID || pickup_ts || PULocationID'
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

),

cleaned_data AS (

    SELECT *
    FROM source_data
    WHERE
        VendorID IS NOT NULL
        AND pickup_ts IS NOT NULL
        AND dropoff_ts IS NOT NULL
        AND trip_distance > 0
        AND fare_amount >= 0
        AND dropoff_ts >= pickup_ts

)

SELECT * FROM cleaned_data
