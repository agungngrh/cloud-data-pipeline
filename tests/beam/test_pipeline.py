from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pytest

from src.streaming.pipeline import (
    ParseEventFn,
    ValidateAndRouteFn,
    _build_output_row,
)

# --- FIXTURES & DUMMY DATA ---


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


# --- 1. UNIT TEST FOR HELPER FUNCTIONS ---


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


# --- 2. UNIT TEST FOR PARSE EVENT FN ---


@patch("scripts.streaming.pipeline.load_pubsub_schema")
@patch("scripts.streaming.pipeline.json_reader")
def test_parse_event_fn_success(mock_reader, mock_load_schema, mock_avro_schema):
    mock_load_schema.return_value = mock_avro_schema
    mock_reader.return_value = iter([{"event_id": "evt-001"}])

    fn = ParseEventFn()
    fn.setup()

    results = list(fn.process(b"dummy_bytes"))

    assert len(results) == 1
    assert results[0]["event_id"] == "evt-001"
    assert "ingestion_time" in results[0]


@patch("scripts.streaming.pipeline.load_pubsub_schema")
@patch("scripts.streaming.pipeline.json_reader")
def test_parse_event_fn_error_handling(mock_reader, mock_load_schema, mock_avro_schema):
    mock_load_schema.return_value = mock_avro_schema
    mock_reader.side_effect = Exception("Corrupted Avro Data")

    fn = ParseEventFn()
    fn.setup()

    # Harus di-catch oleh Exception handler dan tidak memutus pipeline (yield kosong)
    results = list(fn.process(b"invalid_bytes"))
    assert len(results) == 0


# --- 3. PIPELINE INTEGRATION TEST (VALIDATION & ROUTING) ---


@patch("scripts.streaming.pipeline.build_validation_result")
@patch("scripts.streaming.pipeline.build_transformation_result")
def test_validate_and_route_fn_clean_routing(
    mock_transform, mock_validate, sample_event
):
    # Mocking event valid
    mock_validate.return_value = {**sample_event, "is_valid": True}
    mock_transform.return_value = {"duration_minutes": 10.5}

    with TestPipeline() as p:
        input_pcoll = p | "CreateInput" >> beam.Create([sample_event])

        results = input_pcoll | "ValidateAndRoute" >> beam.ParDo(
            ValidateAndRouteFn()
        ).with_outputs("clean", "quarantine")

        # Verifikasi bahwa output masuk ke cabang 'clean'
        def check_clean_output(actual):
            assert len(actual) == 1
            assert actual[0]["event_id"] == "evt-001"
            assert actual[0]["_data_source"] == "streaming"

        assert_that(results.clean, check_clean_output, label="CheckClean")
        assert_that(results.quarantine, equal_to([]), label="CheckQuarantineEmpty")


@patch("scripts.streaming.pipeline.build_validation_result")
def test_validate_and_route_fn_quarantine_routing(mock_validate, sample_event):
    # Mocking event invalid
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

        # Verifikasi bahwa output masuk ke cabang 'quarantine'
        def check_quarantine_output(actual):
            assert len(actual) == 1
            assert actual[0]["event_id"] == "evt-001"

        assert_that(results.clean, equal_to([]), label="CheckCleanEmpty")
        assert_that(
            results.quarantine, check_quarantine_output, label="CheckQuarantine"
        )
