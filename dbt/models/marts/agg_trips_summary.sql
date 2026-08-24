{{ config(materialized='table') }}

with trips as (
    select *
    from {{ ref('fct_trips') }}
)

select
    pickup_date,
    _data_source as data_source,
    count(*) as total_trips,
    sum(passenger_count) as total_passengers,
    avg(passenger_count) as avg_passenger_count,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    sum(tip_amount) as total_tip_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    current_timestamp() as marts_last_refreshed_at

from trips
group by
    pickup_date,
    _data_source
order by pickup_date