{{ config(
    materialized='incremental',
    unique_key=['reporting_period', 'rule_name'],
    incremental_strategy='merge'
) }}

{% set rules = [
    'is_complete_record',
    'is_valid_distance', 'is_valid_fare', 'is_valid_passenger_count',
    'is_valid_location'
] %}

with flagged as (
    select * from {{ ref('int_trips_flagged') }}
)

{% for rule in rules %}
select
    '{{ var("reporting_year_month") }}' as reporting_period,
    '{{ rule }}' as rule_name,
    count(*) as total_rows,
    countif(not {{ rule }}) as failed_rows,
    round(safe_divide(countif(not {{ rule }}), count(*)), 4) as failed_pct,
    current_timestamp() as run_at
from flagged
{% if not loop.last %}union all{% endif %}
{% endfor %}