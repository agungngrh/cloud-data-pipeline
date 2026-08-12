{{ config(materialized='view') }}

select * except (is_valid, quarantine_reason, amount_mismatch,
                  is_complete_record,
                  is_valid_distance, is_valid_fare, is_valid_passenger_count,
                  is_valid_location, is_duplicate, total_amount_recomputed)
from {{ ref('int_trips_flagged') }}
where is_valid = true