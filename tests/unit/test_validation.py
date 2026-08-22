from unittest.mock import patch

import pytest

# Mocking constants sebelum mengimpor fungsi validasi
with patch("config.constants.REQUIRED_FIELDS", ["event_id", "vendor_id"]), patch(
    "config.constants.INVALID_LOCATION_IDS", {264, 265}
), patch(
    "config.constants.AMOUNT_COMPONENT_FIELDS",
    ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount"],
):
    from utils.validation import (
        build_validation_result,
        is_amount_mismatch,
        is_complete_record,
        is_valid_distance,
        is_valid_fare,
        is_valid_location,
        is_valid_passenger_count,
    )


# --- FIXTURES ---
@pytest.fixture
def valid_event():
    """Fixture event dasar yang memenuhi semua kriteria validasi"""
    return {
        "event_id": "evt-123",
        "vendor_id": 1,
        "trip_distance": 2.5,
        "fare_amount": 10.0,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "total_amount": 13.0,
        "passenger_count": 2,
        "pu_location_id": 100,
        "do_location_id": 101,
    }


# --- UNIT TESTS FOR INDIVIDUAL VALIDATORS ---


def test_is_complete_record(valid_event):
    assert is_complete_record(valid_event) is True

    # Missing required field
    incomplete_event = {"event_id": "evt-123"}
    assert is_complete_record(incomplete_event) is False

    # Required field is None
    none_event = {"event_id": "evt-123", "vendor_id": None}
    assert is_complete_record(none_event) is False


@pytest.mark.parametrize(
    "distance, expected",
    [
        (5.0, True),
        (0.0, True),  # Distance 0 tetep valid (non-negative)
        (-1.0, False),  # Negative distance
        (None, False),  # Missing distance
    ],
)
def test_is_valid_distance(valid_event, distance, expected):
    valid_event["trip_distance"] = distance
    assert is_valid_distance(valid_event) is expected


@pytest.mark.parametrize(
    "fare, total, expected",
    [
        (10.0, 12.0, True),
        (0.0, 0.0, True),
        (-5.0, 10.0, False),
        (10.0, -2.0, False),
        (None, 10.0, False),
        (10.0, None, False),
    ],
)
def test_is_valid_fare(valid_event, fare, total, expected):
    valid_event["fare_amount"] = fare
    valid_event["total_amount"] = total
    assert is_valid_fare(valid_event) is expected


@pytest.mark.parametrize(
    "count, expected",
    [
        (1, True),
        (4, True),
        (0, False),  # Passenger count minimal 1
        (-1, False),
        (None, False),
    ],
)
def test_is_valid_passenger_count(valid_event, count, expected):
    valid_event["passenger_count"] = count
    assert is_valid_passenger_count(valid_event) is expected


@pytest.mark.parametrize(
    "pu_id, do_id, expected",
    [
        (100, 101, True),
        (264, 101, False),  # 264 is in INVALID_LOCATION_IDS
        (100, 265, False),  # 265 is in INVALID_LOCATION_IDS
        (None, 101, False),
        (100, None, False),
    ],
)
def test_is_valid_location(valid_event, pu_id, do_id, expected):
    valid_event["pu_location_id"] = pu_id
    valid_event["do_location_id"] = do_id
    assert is_valid_location(valid_event) is expected


def test_is_amount_mismatch(valid_event):
    # Total match: 10.0 + 0.5 + 0.5 + 2.0 + 0.0 = 13.0
    assert is_amount_mismatch(valid_event) is False

    # Mismatch total
    valid_event["total_amount"] = 99.0
    assert is_amount_mismatch(valid_event) is True

    # None total
    valid_event["total_amount"] = None
    assert is_amount_mismatch(valid_event) is True


# --- INTEGRATION TEST FOR BUILD_VALIDATION_RESULT ---


def test_build_validation_result_success(valid_event):
    result = build_validation_result(valid_event)

    assert result["is_valid"] is True
    assert result["quarantine_reason"] is None
    assert result["is_complete_record"] is True
    assert result["is_valid_distance"] is True
    assert result["is_valid_fare"] is True
    assert result["is_valid_passenger_count"] is True
    assert result["is_valid_location"] is True
    assert result["amount_mismatch"] is False
    assert "dq_checked_at" in result


def test_build_validation_result_quarantine(valid_event):
    # Buat event gagal di multiple rules
    valid_event["trip_distance"] = -5.0  # invalid_distance
    valid_event["passenger_count"] = 0  # invalid_passenger_count

    result = build_validation_result(valid_event)

    assert result["is_valid"] is False
    assert "invalid_distance" in result["quarantine_reason"]
    assert "invalid_passenger_count" in result["quarantine_reason"]
    assert result["is_valid_distance"] is False
    assert result["is_valid_passenger_count"] is False
