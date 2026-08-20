{{ config(materialized='table') }}

with trips as (
    select *
    from {{ ref('unified_trips') }}
)

select
    pickup_date,
    _data_source as data_source,
    pickup_hour,
    time_period,
    is_weekend,
    count(*) as total_trips,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    sum(passenger_count) as total_passengers,
    avg(passenger_count) as avg_passenger_count,
    current_timestamp() as marts_last_refreshed_at

from trips
group by
    pickup_date,
    _data_source,
    pickup_hour,
    time_period,
    is_weekend

order by
    pickup_date,
    pickup_hour