import sys
import uuid
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
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from src.config.settings import (
    BQ_TABLE_STREAM_CLEAN,
    BQ_TABLE_STREAM_QUARANTINE,
    SUBSCRIPTION_PATH,
)
from src.core.schema import SCHEMA_CLEAN_TRIPS, SCHEMA_QUARANTINE_TRIPS
from src.core.transformation import build_transformation_result
from src.core.validation import build_validation_result
from src.observability.logger import get_logger
from src.observability.ops_logger import PipelineRunLog, log_pipeline_run
from src.streaming.avro_schema import load_pubsub_schema

logger = get_logger(__name__)

CLEAN_FIELD_NAMES = [field.name for field in SCHEMA_CLEAN_TRIPS]
QUARANTINE_FIELD_NAMES = [field.name for field in SCHEMA_QUARANTINE_TRIPS]


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


def get_written_counts(
    start_time: datetime,
    end_time: datetime,
) -> tuple[int, int]:
    client = bigquery.Client()

    query = f"""
    SELECT
      COUNTIF(source = 'clean') AS rows_written,
      COUNTIF(source = 'quarantine') AS rows_quarantined
    FROM (
      SELECT 'clean' AS source
      FROM `{BQ_TABLE_STREAM_CLEAN}`
      WHERE _data_source = 'streaming'
        AND _loaded_at >= @start_time
        AND _loaded_at <= @end_time

      UNION ALL

      SELECT 'quarantine' AS source
      FROM `{BQ_TABLE_STREAM_QUARANTINE}`
      WHERE _data_source = 'streaming'
        AND _loaded_at >= @start_time
        AND _loaded_at <= @end_time
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )

    row = next(client.query(query, job_config=job_config).result())

    return int(row.rows_written), int(row.rows_quarantined)


def run(argv: list[str] | None = None) -> None:
    """
    Execute the Apache Beam streaming ingestion pipeline with Ops Logging
    """
    run_id = f"stream_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)
    status = "SUCCESS"
    error_msg = None

    rows_read = 0
    rows_written = 0
    rows_quarantined = 0
    result = None

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logger.info(f"Starting Apache Beam streaming pipeline [run_id={run_id}]")

    try:
        pipeline = beam.Pipeline(options=pipeline_options)

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

        result = pipeline.run()
        result.wait_until_finish()

    except KeyboardInterrupt:
        logger.info("Streaming pipeline stopped manually by user (KeyboardInterrupt).")
        status = "SUCCESS"
    except Exception as err:
        status = "FAILED"
        error_msg = str(err)
        logger.error(f"Streaming pipeline failed with error: {err}")
        raise

    finally:
        end_time = datetime.now(timezone.utc)

        try:
            rows_written, rows_quarantined = get_written_counts(
                start_time=start_time,
                end_time=end_time,
            )
            rows_read = rows_written + rows_quarantined

        except GoogleCloudError as metric_err:
            logger.warning(f"Failed to retrieve BigQuery row counts: {metric_err}")

        logger.info(
            f"Recording streaming pipeline ops log to BigQuery [status={status}, rows_read={rows_read}, rows_written={rows_written}, rows_quarantined={rows_quarantined}]"
        )

        log_obj = PipelineRunLog(
            run_id=run_id,
            pipeline_name="beam_streaming_processor",
            pipeline_type="STREAMING",
            task_id="pubsub_to_bigquery_streaming",
            start_time=start_time,
            end_time=end_time,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=rows_quarantined,
            error_message=error_msg,
        )

        log_pipeline_run(log_obj)


if __name__ == "__main__":
    run(sys.argv[1:])
