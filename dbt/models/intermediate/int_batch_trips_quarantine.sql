{{ config(
    materialized='incremental',
    unique_key=['trip_id', 'dup_seq'],
    incremental_strategy='merge'
) }}

with flagged as (
    select * from {{ ref('int_batch_trips_flagged') }}
    where is_valid = false
),

numbered as (
    select
        *,
        row_number() over (partition by trip_id order by fare_amount, trip_distance) as dup_seq
    from flagged
)

select
    *,
    current_timestamp() as dq_checked_at
from numbered