{{ config(materialized='ephemeral') }}

with source as (
    select * from {{ ref('stg_batch_trips') }}
    where source_period = '{{ var("reporting_year_month") }}'
),

recomputed as (
    select
        {{ generate_trip_id() }} as trip_id,
        *,
        coalesce(fare_amount, 0) + coalesce(extra, 0) + coalesce(mta_tax, 0)
            + coalesce(tip_amount, 0) + coalesce(tolls_amount, 0)
            + coalesce(improvement_surcharge, 0) + coalesce(congestion_surcharge, 0)
            + coalesce(cbd_congestion_fee, 0)
            as total_amount_recomputed
    from source
),

flagged as (
    select
        *,
        (vendor_id is not null and rate_code_id is not null
            and passenger_count is not null and payment_type is not null
            and trip_type is not null) as is_complete_record,
        (trip_distance >= 0) as is_valid_distance,
        (fare_amount >= 0 and total_amount >= 0) as is_valid_fare,
        (passenger_count >= 1) as is_valid_passenger_count,
        (pu_location_id not in (264, 265)
            and do_location_id not in (264, 265)) as is_valid_location,
        (round(total_amount, 2) != round(total_amount_recomputed, 2)) as amount_mismatch,
        (count(*) over (
            partition by vendor_id, pickup_datetime, dropoff_datetime,
                         pu_location_id, do_location_id
        ) > 1) as is_duplicate
    from recomputed
),

final as (
    select
        *,
        (is_complete_record
            and is_valid_distance and is_valid_fare
            and is_valid_passenger_count and is_valid_location
            and not is_duplicate) as is_valid
    from flagged
)

select
    *,
    array_to_string(array(
        select reason from unnest([
            case when not is_complete_record then 'incomplete_record' end,
            case when not is_valid_distance then 'invalid_distance' end,
            case when not is_valid_fare then 'invalid_fare' end,
            case when not is_valid_passenger_count then 'invalid_passenger_count' end,
            case when not is_valid_location then 'invalid_location' end,
            case when is_duplicate then 'duplicate' end
        ]) as reason
        where reason is not null
    ), ', ') as quarantine_reason
from final