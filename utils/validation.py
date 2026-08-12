from datetime import datetime, timezone
from typing import Any

from config.constants import (
    AMOUNT_COMPONENT_FIELDS,
    INVALID_LOCATION_IDS,
    REQUIRED_FIELDS,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def is_complete_record(event: dict[str, Any]) -> bool:
    """
    Verify that all required fields are present and non-null
    """
    return all(event.get(field) is not None for field in REQUIRED_FIELDS)


def is_valid_distance(event: dict[str, Any]) -> bool:
    """
    Verify that the trip distance is non-negative
    """
    distance = event.get("trip_distance")
    return distance is not None and distance >= 0


def is_valid_fare(event: dict[str, Any]) -> bool:
    """
    Verify that fare amount and total amount are non-negative
    """
    fare = event.get("fare_amount")
    total = event.get("total_amount")
    if fare is None or total is None:
        return False
    return fare >= 0 and total >= 0


def is_valid_passenger_count(event: dict[str, Any]) -> bool:
    """
    Verify that passenger count is at least one
    """
    count = event.get("passenger_count")
    return count is not None and count >= 1


def is_valid_location(event: dict[str, Any]) -> bool:
    """
    Verify pickup and dropoff locations are present and valid
    """
    pu_id = event.get("pu_location_id")
    do_id = event.get("do_location_id")
    if pu_id is None or do_id is None:
        return False
    return pu_id not in INVALID_LOCATION_IDS and do_id not in INVALID_LOCATION_IDS


def is_amount_mismatch(event: dict[str, Any]) -> bool:
    """
    Check if total_amount matches the sum of individual fare components
    """
    total = event.get("total_amount")
    if total is None:
        return True

    recomputed = sum(
        float(event.get(field) or 0.0) for field in AMOUNT_COMPONENT_FIELDS
    )
    return round(float(total), 2) != round(recomputed, 2)


VALIDATION_RULES = [
    ("is_complete_record", "incomplete_record", is_complete_record),
    ("is_valid_distance", "invalid_distance", is_valid_distance),
    ("is_valid_fare", "invalid_fare", is_valid_fare),
    ("is_valid_passenger_count", "invalid_passenger_count", is_valid_passenger_count),
    ("is_valid_location", "invalid_location", is_valid_location),
]


def build_validation_result(event: dict[str, Any]) -> dict[str, Any]:
    """
    Execute quality checks on an event and append data quality metadata
    """
    rule_results: dict[str, bool] = {}
    failed_reasons: list[str] = []

    for rule_key, reason_code, rule_fn in VALIDATION_RULES:
        passed = rule_fn(event)
        rule_results[rule_key] = passed
        if not passed:
            failed_reasons.append(reason_code)

    is_valid = not failed_reasons
    quarantine_reason = ", ".join(failed_reasons) if failed_reasons else None

    if not is_valid:
        logger.info(
            f"Event quarantined [event_id={event.get('event_id')}, reason={quarantine_reason}]"
        )

    return {
        **event,
        "is_valid": is_valid,
        **rule_results,
        "amount_mismatch": is_amount_mismatch(event),
        "quarantine_reason": quarantine_reason,
        "dq_checked_at": datetime.now(timezone.utc).isoformat(),
    }
