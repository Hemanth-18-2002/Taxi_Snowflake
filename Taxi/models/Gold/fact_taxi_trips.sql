{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'trip_id',
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (

    SELECT
        {{ generate_surrogate_key([
            'VendorID',
            'pickup_ts',
            'PULocationID',
            'DOLocationID'
        ]) }} AS trip_id,

        pickup_ts,
        dropoff_ts,

        VendorID,
        payment_type,
        PULocationID,
        DOLocationID,

        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        Airport_fee,
        cbd_congestion_fee,
        total_amount

    FROM {{ ref('silver_taxi_trips') }}

    {% if is_incremental() %}
      WHERE pickup_ts >= (
        SELECT DATEADD(day, -3, MAX(pickup_ts))
        FROM {{ this }}
      )
    {% endif %}
),

final AS (

    SELECT
        b.trip_id,
        b.pickup_ts,
        b.dropoff_ts,

        dv.vendor_key,
        dp.payment_key,

        dpu.location_key AS pickup_location_key,
        ddo.location_key AS dropoff_location_key,

        b.passenger_count,
        b.trip_distance,
        b.fare_amount,
        b.extra,
        b.mta_tax,
        b.tip_amount,
        b.tolls_amount,
        b.improvement_surcharge,
        b.congestion_surcharge,
        b.Airport_fee,
        b.cbd_congestion_fee,
        b.total_amount

    FROM base b

    -- ✅ Vendor dimension (SCD-0)
   -- Vendor dimension
LEFT JOIN {{ ref('dim_vendor') }} dv
  ON b.VendorID = dv.vendor_id


    -- ✅ Payment Type dimension (SCD-0)
    LEFT JOIN {{ ref('dim_payment') }} dp
        ON b.payment_type = dp.payment_type

    -- ✅ SCD-2 JOIN: Pickup Location
    LEFT JOIN {{ ref('dim_location') }} dpu
        ON b.PULocationID = dpu.LocationID
       AND b.pickup_ts BETWEEN
           dpu.effective_start_date AND dpu.effective_end_date

    -- ✅ SCD-2 JOIN: Dropoff Location
    LEFT JOIN {{ ref('dim_location') }} ddo
        ON b.DOLocationID = ddo.LocationID
       AND b.pickup_ts BETWEEN
           ddo.effective_start_date AND ddo.effective_end_date
)

SELECT * FROM final
