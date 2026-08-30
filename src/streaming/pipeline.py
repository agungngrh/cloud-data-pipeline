import glob
import sys
import uuid
from datetime import datetime, timezone
from time import sleep

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from google.cloud import bigquery

from src.config.settings import settings
from src.observability.logger import get_logger
from src.streaming.monitoring import (
    get_written_counts,
    log_operational_run,
)
from src.streaming.transforms import (
    ParseEventFn,
    ValidateAndRouteFn,
)

logger = get_logger(__name__)

BIGQUERY_VISIBILITY_DELAY_SECONDS = 15


def _build_pipeline_options(argv: list[str] | None) -> PipelineOptions:
    args = argv or []

    if not any(arg == "--runner=DataflowRunner" for arg in args):
        return PipelineOptions(args)

    wheel_matches = glob.glob("dist/cloud_data_pipeline-*-py3-none-any.whl")
    job_name = f"cp3-agungnugraha-streaming-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    dataflow_options = [
        f"--project={settings.gcp_project_id}",
        f"--region={settings.gcp_region}",
        f"--temp_location={settings.gcs_temp_location}",
        f"--staging_location={settings.gcs_staging_location}",
        f"--requirements_file={settings.streaming_requirements_file}",
        f"--job_name={job_name}",
    ]
    if wheel_matches:
        dataflow_options.append(f"--extra_package={wheel_matches[0]}")

    return PipelineOptions(dataflow_options + args)


def run(argv: list[str] | None = None) -> None:
    """
    Build and execute the Apache Beam streaming ingestion pipeline.
    """
    run_id = f"stream_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)

    status = "SUCCESS"
    error_message = None

    rows_read = 0
    rows_written = 0
    rows_quarantined = 0

    pipeline_options = _build_pipeline_options(argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logger.info("Starting Apache Beam streaming pipeline [run_id=%s]", run_id)

    try:
        pipeline = beam.Pipeline(options=pipeline_options)

        parsed_results = (
            pipeline
            | "ReadFromPubSub"
            >> ReadFromPubSub(subscription=settings.subscription_path)
            | "ParseAvroJSON"
            >> beam.ParDo(ParseEventFn()).with_outputs("parsed", "quarantine")
        )

        routed_results = parsed_results.parsed | "ValidateAndRoute" >> beam.ParDo(
            ValidateAndRouteFn()
        ).with_outputs("clean", "quarantine")

        combined_quarantine = (
            parsed_results.quarantine,
            routed_results.quarantine,
        ) | "MergeQuarantineStreams" >> beam.Flatten()

        _ = routed_results.clean | "WriteCleanToBigQuery" >> WriteToBigQuery(
            table=settings.bq_table_stream_clean,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        _ = combined_quarantine | "WriteQuarantineToBigQuery" >> WriteToBigQuery(
            table=settings.bq_table_stream_quarantine,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        pipeline.run().wait_until_finish()

    except KeyboardInterrupt:
        status = "STOPPED"
        logger.info("Streaming pipeline stopped manually")

    except Exception as err:
        status = "FAILED"
        error_message = str(err)
        logger.exception("Streaming pipeline failed")
        raise

    finally:
        sleep(BIGQUERY_VISIBILITY_DELAY_SECONDS)

        end_time = datetime.now(timezone.utc)

        try:
            client = bigquery.Client()

            rows_written, rows_quarantined = get_written_counts(
                client=client,
                start_time=start_time,
                end_time=end_time,
            )

            rows_read = rows_written + rows_quarantined

        except Exception as err:  # noqa: BLE001
            logger.warning("Failed to retrieve BigQuery row counts: %s", err)

        log_operational_run(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status=status,
            error_message=error_message,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=rows_quarantined,
        )


if __name__ == "__main__":
    run(sys.argv[1:])
