# tests/unit/test_event_generator.py
from datetime import datetime

from src.streaming.event_generator import (
    compute_dropoff_datetime,
    compute_fare_amount,
    compute_tip_amount,
    compute_total_amount,
)

# --- compute_fare_amount ---


def test_compute_fare_amount_uses_fixed_rate():
    profile = {"fare_per_mile": {"p10": 2.0, "p90": 2.0}}
    assert compute_fare_amount(trip_distance=5.0, profile=profile) == 10.0


def test_compute_fare_amount_zero_distance():
    profile = {"fare_per_mile": {"p10": 3.5, "p90": 3.5}}
    assert compute_fare_amount(trip_distance=0.0, profile=profile) == 0.0


# --- compute_tip_amount ---


def test_tip_zero_when_not_credit_card():
    profile = {"tip_per_fare": {"p10": 0.15, "p90": 0.15}}
    assert compute_tip_amount(fare_amount=20.0, payment_type=2, profile=profile) == 0.0


def test_tip_zero_when_payment_type_none():
    profile = {"tip_per_fare": {"p10": 0.15, "p90": 0.15}}
    assert (
        compute_tip_amount(fare_amount=20.0, payment_type=None, profile=profile) == 0.0
    )


def test_tip_zero_when_fare_amount_none():
    profile = {"tip_per_fare": {"p10": 0.15, "p90": 0.15}}
    assert compute_tip_amount(fare_amount=None, payment_type=1, profile=profile) == 0.0


def test_tip_zero_when_fare_amount_zero_or_negative():
    profile = {"tip_per_fare": {"p10": 0.15, "p90": 0.15}}
    assert compute_tip_amount(fare_amount=0, payment_type=1, profile=profile) == 0.0
    assert compute_tip_amount(fare_amount=-5, payment_type=1, profile=profile) == 0.0


def test_tip_calculated_for_credit_card():
    profile = {"tip_per_fare": {"p10": 0.15, "p90": 0.15}}
    assert compute_tip_amount(fare_amount=10.0, payment_type=1, profile=profile) == 1.5


# --- compute_dropoff_datetime ---


def test_dropoff_datetime_adds_correct_duration():
    pickup = datetime(2026, 6, 1, 10, 0, 0)
    profile = {"speed_distribution": {"p10": 30.0, "p90": 30.0}}
    dropoff = compute_dropoff_datetime(
        pickup_datetime=pickup, trip_distance=5.0, profile=profile
    )
    assert dropoff == datetime(2026, 6, 1, 10, 10, 0)  # 5 mil @ 30 mph = 10 menit


def test_dropoff_datetime_has_minimum_one_minute_floor():
    pickup = datetime(2026, 6, 1, 10, 0, 0)
    profile = {"speed_distribution": {"p10": 60.0, "p90": 60.0}}
    dropoff = compute_dropoff_datetime(
        pickup_datetime=pickup, trip_distance=0.01, profile=profile
    )
    assert dropoff == datetime(
        2026, 6, 1, 10, 1, 0
    )  # durasi hitung <1 menit, wajib floor ke 1


# --- compute_total_amount ---


def test_compute_total_amount_sums_all_components():
    total = compute_total_amount(
        fare_amount=10.0,
        extra=1.0,
        mta_tax=0.5,
        tip_amount=2.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        congestion_surcharge=2.5,
        cbd_congestion_fee=0.75,
    )
    assert total == 17.05


def test_compute_total_amount_rounds_to_two_decimals():
    total = compute_total_amount(
        fare_amount=10.111,
        extra=0,
        mta_tax=0,
        tip_amount=0,
        tolls_amount=0,
        improvement_surcharge=0,
        congestion_surcharge=0,
        cbd_congestion_fee=0,
    )
    assert total == 10.11
