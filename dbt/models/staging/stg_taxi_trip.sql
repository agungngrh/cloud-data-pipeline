with source as (
    select * from {{ source('raw', 'raw_taxi_trip') }}
)

select

    format_date('%Y-%m', date(lpep_pickup_datetime)) as source_period,

    VendorID as vendor_id,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID as rate_code_id,
    PULocationID as pu_location_id,
    DOLocationID as do_location_id,
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

    current_timestamp() as _loaded_at,
    'batch' as _data_source

from source