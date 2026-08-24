{{ config(materialized='view') }}

with source as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from source