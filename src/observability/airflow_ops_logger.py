from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from src.config.settings import (
    BQ_DATASET_INTERMEDIATE,
    BQ_DATASET_MARTS,
    BQ_DATASET_STAGING,
    BQ_TABLE_TRIP_RAW,
    GCP_PROJECT_ID,
)
from src.observability.logger import get_logger
from src.observability.ops_logger import PipelineRunLog, log_pipeline_run

logger = get_logger(__name__)


def _count_rows(
    client: bigquery.Client,
    table_id: str,
    timestamp_column: str,
    year_month: str,
) -> int:
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{table_id}`
        WHERE FORMAT_TIMESTAMP('%Y-%m', {timestamp_column}) = '{year_month}'
    """
    return next(iter(client.query(query)))["cnt"]


def _get_rows_count(
    client: bigquery.Client, task_id: str, year_month: str
) -> tuple[int | None, int | None, int | None]:
    rows_read = None
    rows_written = None
    rows_quarantined = None

    if "load_trip_data_raw" in task_id:
        rows_read = _count_rows(
            client, BQ_TABLE_TRIP_RAW, "lpep_pickup_datetime", year_month
        )
        rows_written = rows_read

    elif "dbt_run_staging" in task_id:
        rows_read = _count_rows(
            client, BQ_TABLE_TRIP_RAW, "lpep_pickup_datetime", year_month
        )
        rows_written = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_batch_trips",
            "pickup_datetime",
            year_month,
        )

    elif "dbt_run_intermediate" in task_id:
        rows_read = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_batch_trips",
            "pickup_datetime",
            year_month,
        )
        rows_written = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_INTERMEDIATE}.int_batch_trips_clean",
            "pickup_datetime",
            year_month,
        )
        rows_quarantined = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_INTERMEDIATE}.int_batch_trips_quarantine",
            "pickup_datetime",
            year_month,
        )

    elif "dbt_run_marts" in task_id:
        rows_read = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_INTERMEDIATE}.int_unified_trips_clean",
            "pickup_datetime",
            year_month,
        )
        rows_written = _count_rows(
            client,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_MARTS}.fct_trips",
            "pickup_datetime",
            year_month,
        )

    return rows_read, rows_written, rows_quarantined


def record_task_log(context: dict, status: str) -> None:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    task_id = task_instance.task_id if task_instance else "unknown_task"
    start_time = (
        task_instance.start_date
        if task_instance and task_instance.start_date
        else (
            dag_run.start_date
            if dag_run and dag_run.start_date
            else datetime.now(timezone.utc)
        )
    )

    rows_read, rows_written, rows_quarantined = None, None, None

    if status == "SUCCESS":
        try:
            data_interval_start = context.get("data_interval_start")
            if data_interval_start:
                client = bigquery.Client()
                year_month = data_interval_start.strftime("%Y-%m")
                rows_read, rows_written, rows_quarantined = _get_rows_count(
                    client=client, task_id=task_id, year_month=year_month
                )
        except GoogleCloudError as err:
            logger.warning(
                "[OpsLogger] Failed to calculate row counts for task %s: %s",
                task_id,
                err,
            )

    log_pipeline_run(
        PipelineRunLog(
            run_id=str(context.get("run_id")),
            pipeline_name=dag.dag_id if dag else "unknown_dag",
            pipeline_type="BATCH",
            task_id=task_id,
            start_time=start_time,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=rows_quarantined,
            error_message=(
                str(context.get("exception")) if status == "FAILED" else None
            ),
        )
    )


def task_success_callback(context):
    record_task_log(context, "SUCCESS")


def task_failure_callback(context):
    record_task_log(context, "FAILED")
