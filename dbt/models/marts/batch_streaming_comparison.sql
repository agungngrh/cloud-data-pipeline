{{ config(materialized='table') }}

with batch_enriched as (
    select * 
    from {{ ref('int_trips_enriched') }}
),

batch_aggr as (
    select
        coalesce(_data_source, 'batch') as data_source, 
        count(*) as trip_count,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(avg(fare_amount), 2) as avg_fare_amount,
        round(avg(total_amount), 2) as avg_total_amount,
        current_timestamp() as marts_last_refreshed_at 
    from batch_enriched
    group by 1
),

streaming_aggr as (
    select
        'streaming' as data_source,
        count(*) as trip_count,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(avg(fare_amount), 2) as avg_fare_amount,
        round(avg(total_amount), 2) as avg_total_amount,
        current_timestamp() as marts_last_refreshed_at
    from {{ source('streaming', 'stream_trips_clean') }}
    group by 1
)

select * from batch_aggr
union all
select * from streaming_aggr