{{ config(materialized='table') }}

select
    location_id,
    borough,
    zone as zone_name,
    service_zone
from {{ ref('stg_taxi_zones') }}