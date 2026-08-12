{{ config(materialized='table') }}

with trips as (
    select *
    from {{ ref('int_trips_enriched') }}
)

select
    payment_type,
    payment_label,
    _data_source as data_source,
    count(*) as total_trips,
    sum(total_amount) as total_revenue,
    sum(tip_amount) as total_tip_amount,
    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(passenger_count) as avg_passenger_count,

    safe_divide(
        sum(tip_amount),
        sum(fare_amount)
    ) as tip_rate,
    current_timestamp() as marts_last_refreshed_at

from trips
group by 
    payment_type,
    payment_label,
    _data_source
order by total_trips desc