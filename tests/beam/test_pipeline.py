from unittest.mock import patch

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.streaming.transforms import (
    ParseEventFn,
    ValidateAndRouteFn,
    _build_output_row,
)


@pytest.fixture
def mock_avro_schema():
    return {
        "type": "record",
        "name": "TripEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "vendor_id", "type": "int"},
            {"name": "trip_distance", "type": "float"},
        ],
    }


@pytest.fixture
def sample_event():
    return {
        "event_id": "evt-001",
        "vendor_id": 1,
        "trip_distance": 3.5,
        "fare_amount": 12.0,
        "total_amount": 15.0,
        "passenger_count": 1,
        "pu_location_id": 100,
        "do_location_id": 101,
    }


def test_build_output_row():
    data = {
        "event_id": "evt-001",
        "vendor_id": 1,
        "unwanted_field": "secret",
    }
    field_names = ["event_id", "vendor_id"]
    loaded_at = "2026-08-22T10:00:00Z"

    row = _build_output_row(data, field_names, loaded_at)

    assert row["event_id"] == "evt-001"
    assert row["vendor_id"] == 1
    assert "unwanted_field" not in row
    assert row["_loaded_at"] == loaded_at
    assert row["_data_source"] == "streaming"


@patch("src.streaming.transforms.load_pubsub_schema")
@patch("src.streaming.transforms.json_reader")
def test_parse_event_fn_success(mock_reader, mock_load_schema, mock_avro_schema):
    mock_load_schema.return_value = mock_avro_schema
    mock_reader.return_value = iter([{"event_id": "evt-001"}])

    fn = ParseEventFn()
    fn.setup()

    results = list(fn.process(b"dummy_bytes"))

    assert len(results) == 1
    assert results[0].tag == "parsed"
    assert results[0].value["event_id"] == "evt-001"
    assert "ingestion_time" in results[0].value


@patch("src.streaming.transforms.load_pubsub_schema")
@patch("src.streaming.transforms.json_reader")
def test_parse_event_fn_routes_corrupt_payload_to_quarantine(
    mock_reader, mock_load_schema, mock_avro_schema
):
    mock_load_schema.return_value = mock_avro_schema
    mock_reader.side_effect = Exception("Corrupted Avro Data")

    fn = ParseEventFn()
    fn.setup()

    results = list(fn.process(b"invalid_bytes"))

    assert len(results) == 1
    assert results[0].tag == "quarantine"

    row = results[0].value
    assert row["is_valid"] is False
    assert "PARSE_ERROR" in row["quarantine_reason"]
    assert "Corrupted Avro Data" in row["quarantine_reason"]
    assert row["raw_payload"] == "invalid_bytes"
    assert row["_data_source"] == "streaming"


@patch("src.streaming.transforms.build_validation_result")
@patch("src.streaming.transforms.build_transformation_result")
def test_validate_and_route_fn_clean_routing(
    mock_transform, mock_validate, sample_event
):
    mock_validate.return_value = {**sample_event, "is_valid": True}
    mock_transform.return_value = {"duration_minutes": 10.5}

    with TestPipeline() as p:
        input_pcoll = p | "CreateInput" >> beam.Create([sample_event])

        results = input_pcoll | "ValidateAndRoute" >> beam.ParDo(
            ValidateAndRouteFn()
        ).with_outputs("clean", "quarantine")

        def check_clean_output(actual):
            assert len(actual) == 1
            assert actual[0]["event_id"] == "evt-001"
            assert actual[0]["_data_source"] == "streaming"

        assert_that(results.clean, check_clean_output, label="CheckClean")
        assert_that(results.quarantine, equal_to([]), label="CheckQuarantineEmpty")


@patch("src.streaming.transforms.build_validation_result")
def test_validate_and_route_fn_quarantine_routing(mock_validate, sample_event):
    mock_validate.return_value = {
        **sample_event,
        "is_valid": False,
        "quarantine_reason": "invalid_distance",
    }

    with TestPipeline() as p:
        input_pcoll = p | "CreateInput" >> beam.Create([sample_event])

        results = input_pcoll | "ValidateAndRoute" >> beam.ParDo(
            ValidateAndRouteFn()
        ).with_outputs("clean", "quarantine")

        def check_quarantine_output(actual):
            assert len(actual) == 1
            assert actual[0]["event_id"] == "evt-001"

        assert_that(results.clean, equal_to([]), label="CheckCleanEmpty")
        assert_that(
            results.quarantine, check_quarantine_output, label="CheckQuarantine"
        )
