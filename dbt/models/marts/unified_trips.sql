{{ config(materialized='table') }}

with batch_side as (

    select
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
        _data_source,

        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,

        pickup_date,
        pickup_hour,
        pickup_day_name,
        is_weekend,

        trip_duration_minutes,
        time_period,
        payment_label,
        store_and_fwd_flag_label

    from {{ ref('int_trips_enriched') }}

),

streaming_side as (

    select
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
        _data_source,

        pu_zone.zone as pickup_zone,
        pu_zone.borough as pickup_borough,
        do_zone.zone as dropoff_zone,
        do_zone.borough as dropoff_borough,

        pickup_date,
        pickup_hour,
        pickup_day_name,
        is_weekend,

        trip_duration_minutes,
        time_period,
        payment_label,
        store_and_fwd_flag_label

    from {{ source('streaming', 'stream_trips_clean') }} as stream_trips_clean

    left join {{ ref('stg_taxi_zone') }} as pu_zone
        on stream_trips_clean.pu_location_id = pu_zone.location_id

    left join {{ ref('stg_taxi_zone') }} as do_zone
        on stream_trips_clean.do_location_id = do_zone.location_id

)

select
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
    _data_source,

    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,

    pickup_date,
    pickup_hour,
    pickup_day_name,
    is_weekend,

    trip_duration_minutes,
    time_period,
    payment_label,
    store_and_fwd_flag_label

from batch_side

union all

select
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
    _data_source,

    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,

    pickup_date,
    pickup_hour,
    pickup_day_name,
    is_weekend,

    trip_duration_minutes,
    time_period,
    payment_label,
    store_and_fwd_flag_label

from streaming_side