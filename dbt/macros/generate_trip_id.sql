{% macro generate_trip_id() %}
    farm_fingerprint(
        concat(
            coalesce(cast(vendor_id as string), ''),
            coalesce(cast(pickup_datetime as string), ''),
            coalesce(cast(dropoff_datetime as string), ''),
            coalesce(cast(pu_location_id as string), ''),
            coalesce(cast(do_location_id as string), ''),
            coalesce(cast(fare_amount as string), '')
        )
    )
{% endmacro %}