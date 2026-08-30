from datetime import datetime

from google.cloud import bigquery

from src.config.settings import settings
from src.observability.logger import get_logger
from src.observability.ops_logger import PipelineRunLog, log_pipeline_run

logger = get_logger(__name__)


def get_written_counts(
    client: bigquery.Client,
    start_time: datetime,
    end_time: datetime,
) -> tuple[int, int]:
    """
    Count rows persisted to clean and quarantine tables.

    rows_read in the operational log represents total persisted output rows;
    it is not the number of Pub/Sub messages physically read by Beam.
    """
    query = f"""
    SELECT
      (
        SELECT COUNT(*)
        FROM `{settings.bq_table_stream_clean}`
        WHERE _data_source = 'streaming'
          AND _loaded_at >= @start_time
          AND _loaded_at < @end_time
      ) AS rows_written,
      (
        SELECT COUNT(*)
        FROM `{settings.bq_table_stream_quarantine}`
        WHERE _data_source = 'streaming'
          AND _loaded_at >= @start_time
          AND _loaded_at < @end_time
      ) AS rows_quarantined
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    row = next(query_job.result())

    return int(row.rows_written), int(row.rows_quarantined)


def log_operational_run(
    *,
    run_id: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    error_message: str | None,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
) -> None:
    """
    Record the streaming pipeline operational run
    """
    logger.info(
        "Recording streaming pipeline ops log "
        "[status=%s, rows_read=%s, rows_written=%s, rows_quarantined=%s]",
        status,
        rows_read,
        rows_written,
        rows_quarantined,
    )

    log_pipeline_run(
        PipelineRunLog(
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
            error_message=error_message,
        )
    )
