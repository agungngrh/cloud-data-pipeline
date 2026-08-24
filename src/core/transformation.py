from datetime import datetime
from typing import Any

from src.config.constants import PAYMENT_LABELS


def _parse_datetime(value: str | None) -> datetime | None:
    """
    Parse an ISO format string into a datetime object
    """
    if not value:
        return None
    return datetime.fromisoformat(value)


def compute_time_period(pickup_hour: int | None) -> str | None:
    """
    Categorize a given hour into a specific time of day
    """
    if pickup_hour is None:
        return None

    if 0 <= pickup_hour <= 5:
        return "Late Night"
    if 6 <= pickup_hour <= 11:
        return "Morning"
    if 12 <= pickup_hour <= 16:
        return "Afternoon"
    if 17 <= pickup_hour <= 20:
        return "Evening"

    return "Night"


def compute_trip_duration_minutes(
    pickup_dt: datetime | None, dropoff_dt: datetime | None
) -> int | None:
    """
    Calculate the total trip duration in minutes
    """
    if not pickup_dt or not dropoff_dt:
        return None
    return round((dropoff_dt - pickup_dt).total_seconds() / 60)


def compute_payment_label(payment_type: int | None) -> str:
    """
    Map a payment type integer to its readable text label
    """
    if payment_type is None:
        return "Unknown"
    return PAYMENT_LABELS.get(payment_type, "Unknown")


def build_transformation_result(event: dict[str, Any]) -> dict[str, Any]:
    """
    Derive and build new fields from raw event data without external queries
    """
    pickup_dt = _parse_datetime(event.get("pickup_datetime"))
    dropoff_dt = _parse_datetime(event.get("dropoff_datetime"))
    pickup_date = pickup_hour = pickup_day_name = is_weekend = None

    if pickup_dt:
        pickup_date = pickup_dt.date().isoformat()
        pickup_hour = pickup_dt.hour
        pickup_day_name = pickup_dt.strftime("%A")
        is_weekend = pickup_dt.weekday() >= 5

    return {
        "pickup_date": pickup_date,
        "pickup_hour": pickup_hour,
        "pickup_day_name": pickup_day_name,
        "is_weekend": is_weekend,
        "trip_duration_minutes": compute_trip_duration_minutes(pickup_dt, dropoff_dt),
        "time_period": compute_time_period(pickup_hour),
        "payment_label": compute_payment_label(event.get("payment_type")),
    }
