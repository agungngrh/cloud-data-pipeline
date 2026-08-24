{{ config(
    materialized='incremental',
    unique_key='trip_id',
    partition_by={
        "field": "pickup_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['pickup_borough', 'dropoff_borough']
) }}

with unified_trips as (
    select * from {{ ref('int_unified_trips_clean') }}
    
    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

taxi_zones as (
    select * from {{ ref('dim_taxi_zones') }}
),

enriched as (
    select
        t.*,
        
        pu_zone.zone_name as pickup_zone,
        pu_zone.borough as pickup_borough,
        do_zone.zone_name as dropoff_zone,
        do_zone.borough as dropoff_borough,

        date(t.pickup_datetime) as pickup_date,
        extract(hour from t.pickup_datetime) as pickup_hour,
        format_timestamp('%A', t.pickup_datetime) as pickup_day_name,
        extract(dayofweek from t.pickup_datetime) in (1, 7) as is_weekend,
        timestamp_diff(t.dropoff_datetime, t.pickup_datetime, minute) as trip_duration_minutes,

        case
            when extract(hour from t.pickup_datetime) between 0 and 5 then 'Late Night'
            when extract(hour from t.pickup_datetime) between 6 and 11 then 'Morning'
            when extract(hour from t.pickup_datetime) between 12 and 16 then 'Afternoon'
            when extract(hour from t.pickup_datetime) between 17 and 20 then 'Evening'
            else 'Night'
        end as time_period,

        case t.payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_label,

        case t.store_and_fwd_flag
            when 'Y' then 'Store and Forward'
            when 'N' then 'Normal'
            else 'Unknown'
        end as store_and_fwd_flag_label

    from unified_trips as t
    left join taxi_zones as pu_zone
        on t.pu_location_id = pu_zone.location_id
    left join taxi_zones as do_zone
        on t.do_location_id = do_zone.location_id
)

select * from enriched