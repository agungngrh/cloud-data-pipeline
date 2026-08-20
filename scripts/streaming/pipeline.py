import json
import sys
from datetime import datetime, timezone
from io import StringIO
from typing import Any

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.pvalue import TaggedOutput
from fastavro import json_reader

from config.constants import AVSC_PATH
from config.settings import (
    BQ_TABLE_STREAM_CLEAN,
    BQ_TABLE_STREAM_QUARANTINE,
    SUBSCRIPTION_PATH,
)
from utils.logger import get_logger
from utils.schema import SCHEMA_CLEAN_TRIPS, SCHEMA_QUARANTINE_TRIPS
from utils.transformation import build_transformation_result
from utils.validation import build_validation_result

logger = get_logger(__name__)

CLEAN_FIELD_NAMES = [field.name for field in SCHEMA_CLEAN_TRIPS]
QUARANTINE_FIELD_NAMES = [field.name for field in SCHEMA_QUARANTINE_TRIPS]


def load_pubsub_schema() -> dict[str, Any]:
    """
    Load the Avro schema used by Pub/Sub messages
    """
    with AVSC_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def _build_output_row(
    data: dict[str, Any], field_names: list[str], loaded_at: str
) -> dict[str, Any]:
    """
    Extract specified fields from event data and append metadata attributes
    """
    row = {name: data[name] for name in field_names if name in data}
    row["_loaded_at"] = loaded_at
    row["_data_source"] = "streaming"
    return row


class ParseEventFn(beam.DoFn):
    """
    Parse incoming JSON Pub/Sub messages and attach ingestion timestamps
    """

    def setup(self) -> None:
        self.schema = load_pubsub_schema()

    def process(self, element: bytes):
        try:
            event = next(json_reader(StringIO(element.decode("utf-8")), self.schema))
            event["ingestion_time"] = datetime.now(timezone.utc).isoformat()
            yield event
        except Exception as err:  # noqa: BLE001
            logger.error(
                f"Failed to parse Pub/Sub message: {type(err).__name__}: {err}"
            )


class ValidateAndRouteFn(beam.DoFn):
    """
    Validate event records and route them to clean or quarantine side outputs
    """

    def process(self, event: dict[str, Any]):
        result = build_validation_result(event)
        current_time = datetime.now(timezone.utc).isoformat()

        if result.get("is_valid"):
            enriched = {**result, **build_transformation_result(event)}
            clean_row = _build_output_row(enriched, CLEAN_FIELD_NAMES, current_time)
            yield TaggedOutput("clean", clean_row)
        else:
            quarantine_row = _build_output_row(
                result, QUARANTINE_FIELD_NAMES, current_time
            )
            yield TaggedOutput("quarantine", quarantine_row)


def run(argv: list[str] | None = None) -> None:
    """
    Execute the Apache Beam streaming ingestion pipeline
    """
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        parsed_events = (
            pipeline
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=SUBSCRIPTION_PATH)
            | "ParseAvroJSON" >> beam.ParDo(ParseEventFn())
        )

        results = parsed_events | "ValidateAndRoute" >> beam.ParDo(
            ValidateAndRouteFn()
        ).with_outputs("clean", "quarantine")

        _ = results.clean | "WriteCleanToBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE_STREAM_CLEAN,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        _ = results.quarantine | "WriteQuarantineToBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE_STREAM_QUARANTINE,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )


if __name__ == "__main__":
    run(sys.argv[1:])
