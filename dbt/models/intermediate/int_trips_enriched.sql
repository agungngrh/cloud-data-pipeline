{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge'
) }}

with clean_trips as (
    select * from {{ ref('int_trips_clean') }}
),

enriched as (
    select
        clean_trips.*,
        pu_zone.zone as pickup_zone,
        pu_zone.borough as pickup_borough,
        do_zone.zone as dropoff_zone,
        do_zone.borough as dropoff_borough,

        date(pickup_datetime) as pickup_date,
        extract(hour from pickup_datetime) as pickup_hour,
        format_timestamp('%A', pickup_datetime) as pickup_day_name,
        extract(dayofweek from pickup_datetime) in (1, 7) as is_weekend,
        timestamp_diff(dropoff_datetime, pickup_datetime, minute) as trip_duration_minutes,

        case
            when extract(hour from pickup_datetime) between 0 and 5 then 'Late Night'
            when extract(hour from pickup_datetime) between 6 and 11 then 'Morning'
            when extract(hour from pickup_datetime) between 12 and 16 then 'Afternoon'
            when extract(hour from pickup_datetime) between 17 and 20 then 'Evening'
            else 'Night'
        end as time_period,

        case payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_label,

        case store_and_fwd_flag
            when 'Y' then 'Store and Forward'
            when 'N' then 'Normal'
            else 'Unknown'
        end as store_and_fwd_flag_label

    from clean_trips
    left join {{ ref('stg_taxi_zone') }} as pu_zone
        on clean_trips.pu_location_id = pu_zone.location_id
    left join {{ ref('stg_taxi_zone') }} as do_zone
        on clean_trips.do_location_id = do_zone.location_id
)

select * from enriched