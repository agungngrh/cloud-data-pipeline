from datetime import datetime, timezone
from io import StringIO
from typing import Any

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from fastavro import json_reader

from src.core.schema import SCHEMA_CLEAN_TRIPS, SCHEMA_QUARANTINE_TRIPS
from src.core.transformation import build_transformation_result
from src.core.validation import build_validation_result
from src.observability.logger import get_logger
from src.streaming.avro_schema import load_pubsub_schema

logger = get_logger(__name__)

CLEAN_FIELD_NAMES = [field.name for field in SCHEMA_CLEAN_TRIPS]
QUARANTINE_FIELD_NAMES = [field.name for field in SCHEMA_QUARANTINE_TRIPS]


def _build_output_row(
    data: dict[str, Any],
    field_names: list[str],
    loaded_at: str,
) -> dict[str, Any]:
    """
    Build a BigQuery-compatible output row with ingestion metadata.
    """
    row = {name: data[name] for name in field_names if name in data}
    row["_loaded_at"] = loaded_at
    row["_data_source"] = "streaming"
    return row


class ParseEventFn(beam.DoFn):
    """
    Parse one Avro JSON event from a Pub/Sub message.
    Emits to 'parsed' on success, 'quarantine' on parse failure.
    """

    def setup(self) -> None:
        self.schema = load_pubsub_schema()

    def process(self, element: bytes):
        current_time = datetime.now(timezone.utc).isoformat()

        try:
            event = next(json_reader(StringIO(element.decode("utf-8")), self.schema))
            event["ingestion_time"] = current_time
            yield TaggedOutput("parsed", event)

        except Exception as err:  # noqa: BLE001
            logger.error(
                "Failed to parse Pub/Sub message: %s: %s",
                type(err).__name__,
                err,
            )
            yield TaggedOutput(
                "quarantine",
                _build_output_row(
                    {
                        "is_valid": False,
                        "quarantine_reason": (
                            f"PARSE_ERROR: {type(err).__name__}: {err}"
                        ),
                        "raw_payload": element.decode("utf-8", errors="replace"),
                        "dq_checked_at": current_time,
                    },
                    QUARANTINE_FIELD_NAMES,
                    current_time,
                ),
            )


class ValidateAndRouteFn(beam.DoFn):
    """
    Validate an event and route it to clean or quarantine output.
    """

    def process(self, event: dict[str, Any]):
        validation_result = build_validation_result(event)
        current_time = datetime.now(timezone.utc).isoformat()

        if validation_result.get("is_valid"):
            enriched_event = {
                **validation_result,
                **build_transformation_result(event),
            }

            yield TaggedOutput(
                "clean",
                _build_output_row(
                    enriched_event,
                    CLEAN_FIELD_NAMES,
                    current_time,
                ),
            )

            return

        yield TaggedOutput(
            "quarantine",
            _build_output_row(
                validation_result,
                QUARANTINE_FIELD_NAMES,
                current_time,
            ),
        )
