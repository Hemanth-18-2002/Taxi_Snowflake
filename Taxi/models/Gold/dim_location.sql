{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'location_key',
    on_schema_change = 'sync_all_columns'
) }}

WITH source_locations AS (

    SELECT DISTINCT PULocationID AS LocationID
    FROM {{ ref('silver_taxi_trips') }}

    UNION

    SELECT DISTINCT DOLocationID AS LocationID
    FROM {{ ref('silver_taxi_trips') }}

),

prepared_source AS (

    SELECT
        LocationID,

        /* Change detection hash */
        {{ generate_surrogate_key(['LocationID']) }} AS record_hash,

        CURRENT_TIMESTAMP() AS effective_start_date,
        TO_TIMESTAMP_NTZ('9999-12-31') AS effective_end_date,
        TRUE AS is_current

    FROM source_locations

),

final AS (

    SELECT
        {{ generate_surrogate_key([
            'LocationID',
            'effective_start_date'
        ]) }} AS location_key,

        LocationID,
        record_hash,
        effective_start_date,
        effective_end_date,
        is_current

    FROM prepared_source

)

SELECT * FROM final
