from datetime import datetime

from src.config.constants import PAYMENT_LABELS
from src.core.transformation import (
    build_transformation_result,
    compute_payment_label,
    compute_time_period,
    compute_trip_duration_minutes,
)


def test_time_period_boundaries():
    assert compute_time_period(0) == "Late Night"
    assert compute_time_period(5) == "Late Night"
    assert compute_time_period(6) == "Morning"
    assert compute_time_period(11) == "Morning"
    assert compute_time_period(12) == "Afternoon"
    assert compute_time_period(16) == "Afternoon"
    assert compute_time_period(17) == "Evening"
    assert compute_time_period(20) == "Evening"
    assert compute_time_period(21) == "Night"
    assert compute_time_period(23) == "Night"


def test_time_period_none_when_hour_missing():
    assert compute_time_period(None) is None


def test_trip_duration_calculates_correctly():
    pickup = datetime(2026, 6, 1, 10, 0, 0)
    dropoff = datetime(2026, 6, 1, 10, 5, 0)
    assert compute_trip_duration_minutes(pickup, dropoff) == 5


def test_trip_duration_none_when_pickup_missing():
    dropoff = datetime(2026, 6, 1, 10, 5, 0)
    assert compute_trip_duration_minutes(None, dropoff) is None


def test_trip_duration_none_when_dropoff_missing():
    pickup = datetime(2026, 6, 1, 10, 0, 0)
    assert compute_trip_duration_minutes(pickup, None) is None


def test_payment_label_known_type():
    known_type, expected_label = next(iter(PAYMENT_LABELS.items()))
    assert compute_payment_label(known_type) == expected_label


def test_payment_label_unknown_type_returns_unknown():
    assert compute_payment_label(9999) == "Unknown"


def test_payment_label_none_returns_unknown():
    assert compute_payment_label(None) == "Unknown"


def test_build_transformation_result_full_event():
    event = {
        "pickup_datetime": "2026-06-06T14:30:00",
        "dropoff_datetime": "2026-06-06T14:45:00",
        "payment_type": next(iter(PAYMENT_LABELS.keys())),
    }
    result = build_transformation_result(event)

    assert result["pickup_date"] == "2026-06-06"
    assert result["pickup_hour"] == 14
    assert result["pickup_day_name"] == "Saturday"
    assert result["is_weekend"] is True
    assert result["trip_duration_minutes"] == 15
    assert result["time_period"] == "Afternoon"
    assert result["payment_label"] != "Unknown"


def test_build_transformation_result_weekday_not_weekend():
    event = {
        "pickup_datetime": "2026-06-01T09:00:00",
        "dropoff_datetime": "2026-06-01T09:10:00",
        "payment_type": None,
    }
    result = build_transformation_result(event)

    assert result["is_weekend"] is False
    assert result["pickup_day_name"] == "Monday"


def test_build_transformation_result_missing_pickup_datetime():
    event = {
        "dropoff_datetime": "2026-06-01T09:10:00",
        "payment_type": None,
    }
    result = build_transformation_result(event)

    assert result["pickup_date"] is None
    assert result["pickup_hour"] is None
    assert result["pickup_day_name"] is None
    assert result["is_weekend"] is None
    assert result["time_period"] is None
    assert result["trip_duration_minutes"] is None


def test_build_transformation_result_missing_dropoff_only():
    event = {
        "pickup_datetime": "2026-06-06T14:30:00",
        "payment_type": None,
    }
    result = build_transformation_result(event)

    assert result["pickup_hour"] == 14
    assert result["trip_duration_minutes"] is None
