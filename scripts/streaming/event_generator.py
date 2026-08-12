import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.constants import (
    BATCH_PROFILING,
    END_DATE,
    START_DATE,
)


def load_profile(path: str | Path = BATCH_PROFILING) -> dict[str, Any]:
    """
    Load the streaming data profile from a JSON file
    """
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def sample_category(profile: dict[str, Any], column: str) -> str:
    """
    Sample a categorical string value based on profiled probability distribution
    """
    distribution = profile["category_distribution"][column]
    values = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(values, weights=weights, k=1)[0]


def sample_category_as_int(profile: dict[str, Any], column: str) -> int | None:
    """
    Sample a categorical value and cast to integer if not null
    """
    value = sample_category(profile, column)
    if value == "null":
        return None
    return int(float(value))


def sample_category_as_str(profile: dict[str, Any], column: str) -> str | None:
    """
    Sample a categorical string value, converting the 'null' marker to None
    """
    value = sample_category(profile, column)
    return None if value == "null" else value


def sample_category_as_float(profile: dict[str, Any], column: str) -> float | None:
    """
    Sample a categorical value and cast to float if not null
    """
    value = sample_category(profile, column)
    if value == "null":
        return None
    return float(value)


def sample_is_incomplete_group(profile: dict[str, Any]) -> bool:
    """
    Determine probabilistically if a correlated group of fields is missing.
    """
    null_rate = profile["category_distribution"]["trip_type"].get("null", 0.0)
    return random.random() < null_rate


def sample_category_non_null_as_int(profile: dict[str, Any], column: str) -> int:
    """
    Sample a categorical integer value, excluding 'null' options.
    """
    distribution = profile["category_distribution"][column]
    values = [v for v in distribution if v != "null"]
    weights = [distribution[v] for v in values]
    value = random.choices(values, weights=weights, k=1)[0]
    return int(float(value))


def sample_category_non_null_as_str(profile: dict[str, Any], column: str) -> str:
    """
    Sample a categorical string value, excluding 'null' options.
    """
    distribution = profile["category_distribution"][column]
    values = [v for v in distribution if v != "null"]
    weights = [distribution[v] for v in values]
    return random.choices(values, weights=weights, k=1)[0]


def sample_category_non_null_as_float(profile: dict[str, Any], column: str) -> float:
    """
    Sample a categorical float value, excluding 'null' options.
    """
    distribution = profile["category_distribution"][column]
    values = [v for v in distribution if v != "null"]
    weights = [distribution[v] for v in values]
    value = random.choices(values, weights=weights, k=1)[0]
    return float(value)


def sample_datetime(
    profile: dict[str, Any], start_date: datetime, end_date: datetime
) -> datetime:
    """
    Generate a random pickup timestamp adhering to profiled hourly distribution
    """
    delta_days = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, delta_days))

    hours = list(profile["hour_distribution"].keys())
    weights = list(profile["hour_distribution"].values())

    hour = int(random.choices(hours, weights=weights, k=1)[0])
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return random_day.replace(hour=hour, minute=minute, second=second)


def sample_trip_distance(profile: dict[str, Any]) -> float:
    """
    Sample a trip distance value from the profiled distribution
    """
    return random.choice(profile["trip_distance_values"])


def compute_fare_amount(trip_distance: float, profile: dict[str, Any]) -> float:
    """
    Calculate base fare using profiled rate per mile
    """
    rate = random.uniform(
        profile["fare_per_mile"]["p10"], profile["fare_per_mile"]["p90"]
    )
    return round(trip_distance * rate, 2)


def compute_tip_amount(
    fare_amount: float | None,
    payment_type: int | None,
    profile: dict[str, Any],
) -> float:
    """
    Calculate tip amount for credit card transactions (payment_type=1)
    """
    if payment_type != 1 or fare_amount is None or fare_amount <= 0:
        return 0.0

    ratio = random.uniform(
        profile["tip_per_fare"]["p10"], profile["tip_per_fare"]["p90"]
    )
    return round(fare_amount * ratio, 2)


def compute_dropoff_datetime(
    pickup_datetime: datetime, trip_distance: float, profile: dict[str, Any]
) -> datetime:
    """
    Calculate drop-off timestamp based on sampled speed range
    """
    speed_mph = random.uniform(
        profile["speed_distribution"]["p10"], profile["speed_distribution"]["p90"]
    )
    duration_minutes = max((trip_distance / speed_mph) * 60, 1)
    return pickup_datetime + timedelta(minutes=duration_minutes)


def compute_total_amount(
    fare_amount: float,
    extra: float,
    mta_tax: float,
    tip_amount: float,
    tolls_amount: float,
    improvement_surcharge: float,
    congestion_surcharge: float,
    cbd_congestion_fee: float,
) -> float:
    """
    Sum all fare components into total amount
    """
    total_amount = (
        fare_amount
        + extra
        + mta_tax
        + tip_amount
        + tolls_amount
        + improvement_surcharge
        + congestion_surcharge
        + cbd_congestion_fee
    )
    return round(total_amount, 2)


def generate_event(
    profile: dict[str, Any], start_date: datetime, end_date: datetime
) -> dict[str, Any]:
    """
    Generate a single synthetic taxi trip event
    """
    pickup_datetime = sample_datetime(
        profile=profile, start_date=start_date, end_date=end_date
    )
    trip_distance = sample_trip_distance(profile)
    dropoff_datetime = compute_dropoff_datetime(
        pickup_datetime=pickup_datetime,
        trip_distance=trip_distance,
        profile=profile,
    )

    vendor_id = sample_category_as_int(profile, "VendorID")

    if sample_is_incomplete_group(profile):
        store_and_fwd_flag = None
        rate_code_id = None
        passenger_count = None
        payment_type = None
        trip_type = None
        congestion_surcharge = 0.0
    else:
        store_and_fwd_flag = sample_category_non_null_as_str(
            profile, "store_and_fwd_flag"
        )
        rate_code_id = sample_category_non_null_as_int(profile, "RatecodeID")
        passenger_count = sample_category_non_null_as_int(profile, "passenger_count")
        payment_type = sample_category_non_null_as_int(profile, "payment_type")
        trip_type = sample_category_non_null_as_int(profile, "trip_type")
        congestion_surcharge = sample_category_non_null_as_float(
            profile, "congestion_surcharge"
        )

    pu_location_id = sample_category_as_int(profile, "PULocationID")
    do_location_id = sample_category_as_int(profile, "DOLocationID")

    fare_amount = compute_fare_amount(trip_distance=trip_distance, profile=profile)
    extra = sample_category_as_float(profile, "extra") or 0.0
    mta_tax = sample_category_as_float(profile, "mta_tax") or 0.0
    improvement_surcharge = (
        sample_category_as_float(profile, "improvement_surcharge") or 0.0
    )
    cbd_congestion_fee = sample_category_as_float(profile, "cbd_congestion_fee") or 0.0
    tolls_amount = sample_category_as_float(profile, "tolls_amount") or 0.0

    tip_amount = compute_tip_amount(
        fare_amount=fare_amount,
        payment_type=payment_type,
        profile=profile,
    )

    total_amount = compute_total_amount(
        fare_amount=fare_amount,
        extra=extra,
        mta_tax=mta_tax,
        tip_amount=tip_amount,
        tolls_amount=tolls_amount,
        improvement_surcharge=improvement_surcharge,
        congestion_surcharge=congestion_surcharge,
        cbd_congestion_fee=cbd_congestion_fee,
    )

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": pickup_datetime.isoformat(),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "vendor_id": vendor_id,
        "rate_code_id": rate_code_id,
        "passenger_count": passenger_count,
        "trip_type": trip_type,
        "payment_type": payment_type,
        "pickup_datetime": pickup_datetime.isoformat(),
        "dropoff_datetime": dropoff_datetime.isoformat(),
        "store_and_fwd_flag": store_and_fwd_flag,
        "pu_location_id": pu_location_id,
        "do_location_id": do_location_id,
        "trip_distance": trip_distance,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "congestion_surcharge": congestion_surcharge,
        "cbd_congestion_fee": cbd_congestion_fee,
        "total_amount": total_amount,
    }


if __name__ == "__main__":
    prof = load_profile()
    event = generate_event(profile=prof, start_date=START_DATE, end_date=END_DATE)
