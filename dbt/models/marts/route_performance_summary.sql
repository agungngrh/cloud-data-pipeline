{{ config(materialized='table') }}

with trips as (
    select *
    from {{ ref('unified_trips') }}
)

select
    pu_location_id as pickup_location_id,
    do_location_id as dropoff_location_id,
    any_value(pickup_zone) as pickup_zone,
    any_value(dropoff_zone) as dropoff_zone,
    _data_source as data_source,

    count(*) as total_trips,
    sum(total_amount) as total_revenue,
    sum(passenger_count) as total_passengers,

    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(passenger_count) as avg_passenger_count,

    current_timestamp() as marts_last_refreshed_at

from trips
group by 
    pu_location_id,
    do_location_id,
    _data_source
order by total_trips desc