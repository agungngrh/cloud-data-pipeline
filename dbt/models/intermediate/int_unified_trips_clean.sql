{{ config(materialized='view') }}

with batch_side as (
    select
        cast(trip_id as string) as trip_id,
        cast(null as string) as event_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        rate_code_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        cbd_congestion_fee,
        _loaded_at,
        _data_source
    from {{ ref('int_batch_trips_clean') }}
),

streaming_side as (
    select
        cast(event_id as string) as trip_id,
        event_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        rate_code_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        cbd_congestion_fee,
        _loaded_at,
        _data_source
    from {{ source('streaming', 'int_stream_trips_clean') }}
)

select * from batch_side
union all
select * from streaming_side