{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('fct_trips') }}
),

pickup_summary as (
    select
        pu_location_id as location_id,
        any_value(pickup_zone) as zone,
        any_value(pickup_borough) as borough,
        count(*) as total_pickup_trips,
        sum(total_amount) as total_revenue,
        avg(fare_amount) as avg_fare_amount,
        avg(tip_amount) as avg_tip_amount,
        avg(trip_distance) as avg_trip_distance,
        avg(trip_duration_minutes) as avg_trip_duration_minutes,
        avg(passenger_count) as avg_passenger_count
    from trips
    group by 1
),

dropoff_summary as (
    select
        do_location_id as location_id,
        any_value(dropoff_zone) as zone,
        any_value(dropoff_borough) as borough,
        count(*) as total_dropoff_trips
    from trips
    group by 1
),

all_active_locations as (
    select location_id from pickup_summary
    union distinct
    select location_id from dropoff_summary
)

select
    loc.location_id,
    
    coalesce(p.zone, d.zone) as zone,
    coalesce(p.borough, d.borough) as borough,

    coalesce(p.total_pickup_trips, 0) as total_pickup_trips,
    coalesce(d.total_dropoff_trips, 0) as total_dropoff_trips,
    coalesce(p.total_revenue, 0) as total_revenue,
    p.avg_fare_amount,
    p.avg_tip_amount,
    p.avg_trip_distance,
    p.avg_trip_duration_minutes,
    p.avg_passenger_count,

    current_timestamp() as marts_last_refreshed_at

from all_active_locations loc
left join pickup_summary p 
    on loc.location_id = p.location_id
left join dropoff_summary d 
    on loc.location_id = d.location_id

order by total_pickup_trips desc