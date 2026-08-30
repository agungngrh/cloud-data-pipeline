from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import GoogleCloudError

from src.observability.airflow_ops_logger import (
    _count_rows,
    _get_rows_count,
    record_task_log,
    task_failure_callback,
    task_success_callback,
)


def test_count_rows_returns_int_from_query_result():
    mock_client = MagicMock()
    mock_row = {"cnt": 150}

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([mock_row])
    mock_client.query.return_value = mock_query_job

    result = _count_rows(
        client=mock_client,
        table_id="project.dataset.table",
        timestamp_column="pickup_datetime",
        year_month="2026-06",
    )

    assert result == 150
    assert isinstance(result, int)


def test_count_rows_passes_year_month_parameter():
    mock_client = MagicMock()
    mock_row = {"cnt": 0}
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([mock_row])
    mock_client.query.return_value = mock_query_job

    _count_rows(
        client=mock_client,
        table_id="project.dataset.table",
        timestamp_column="pickup_datetime",
        year_month="2026-06",
    )

    _, kwargs = mock_client.query.call_args
    job_config = kwargs["job_config"]
    param = job_config.query_parameters[0]

    assert param.name == "year_month"
    assert param.value == "2026-06"


@patch("src.observability.airflow_ops_logger._count_rows")
def test_get_rows_count_load_trip_data_raw(mock_count_rows):
    mock_count_rows.return_value = 100

    rows_read, rows_written, rows_quarantined = _get_rows_count(
        client=MagicMock(),
        task_id="ingestion_layer.load_trip_data_raw",
        year_month="2026-06",
    )

    assert rows_read == 100
    assert rows_written == 100
    assert rows_quarantined is None


@patch("src.observability.airflow_ops_logger._count_rows")
def test_get_rows_count_dbt_run_staging(mock_count_rows):
    mock_count_rows.side_effect = [100, 95]

    rows_read, rows_written, rows_quarantined = _get_rows_count(
        client=MagicMock(),
        task_id="staging_layer.dbt_run_staging",
        year_month="2026-06",
    )

    assert rows_read == 100
    assert rows_written == 95
    assert rows_quarantined is None


@patch("src.observability.airflow_ops_logger._count_rows")
def test_get_rows_count_dbt_run_intermediate_includes_quarantine(mock_count_rows):
    mock_count_rows.side_effect = [95, 90, 5]

    rows_read, rows_written, rows_quarantined = _get_rows_count(
        client=MagicMock(),
        task_id="intermediate_layer.dbt_run_intermediate",
        year_month="2026-06",
    )

    assert rows_read == 95
    assert rows_written == 90
    assert rows_quarantined == 5


@patch("src.observability.airflow_ops_logger._count_rows")
def test_get_rows_count_dbt_run_marts(mock_count_rows):
    mock_count_rows.side_effect = [90, 90]

    rows_read, rows_written, rows_quarantined = _get_rows_count(
        client=MagicMock(),
        task_id="marts_layer.dbt_run_marts",
        year_month="2026-06",
    )

    assert rows_read == 90
    assert rows_written == 90
    assert rows_quarantined is None


def test_get_rows_count_unknown_task_id_returns_all_none():
    rows_read, rows_written, rows_quarantined = _get_rows_count(
        client=MagicMock(),
        task_id="ingestion_layer.check_trip_file",
        year_month="2026-06",
    )

    assert rows_read is None
    assert rows_written is None
    assert rows_quarantined is None


@patch("src.observability.airflow_ops_logger.log_pipeline_run")
@patch("src.observability.airflow_ops_logger._get_rows_count")
@patch("src.observability.airflow_ops_logger.bigquery.Client")
def test_record_task_log_success_calculates_row_counts(
    mock_bq_client, mock_get_rows_count, mock_log_pipeline_run
):
    mock_get_rows_count.return_value = (100, 95, 5)

    mock_task_instance = MagicMock()
    mock_task_instance.task_id = "staging_layer.dbt_run_staging"
    mock_task_instance.start_date = datetime.now(timezone.utc)

    mock_dag = MagicMock()
    mock_dag.dag_id = "agungnugraha_batch_trip_pipeline"

    context = {
        "task_instance": mock_task_instance,
        "dag_run": MagicMock(),
        "dag": mock_dag,
        "run_id": "manual__2026-06-01",
        "data_interval_start": datetime(2026, 6, 1, tzinfo=timezone.utc),
    }

    record_task_log(context, "SUCCESS")

    mock_log_pipeline_run.assert_called_once()
    logged_entry = mock_log_pipeline_run.call_args[0][0]

    assert logged_entry.status == "SUCCESS"
    assert logged_entry.rows_read == 100
    assert logged_entry.rows_written == 95
    assert logged_entry.rows_quarantined == 5
    assert logged_entry.error_message is None


@patch("src.observability.airflow_ops_logger.log_pipeline_run")
def test_record_task_log_failure_skips_row_counting(mock_log_pipeline_run):
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = "ingestion_layer.load_trip_data_raw"
    mock_task_instance.start_date = datetime.now(timezone.utc)

    mock_dag = MagicMock()
    mock_dag.dag_id = "agungnugraha_batch_trip_pipeline"

    context = {
        "task_instance": mock_task_instance,
        "dag_run": MagicMock(),
        "dag": mock_dag,
        "run_id": "manual__2026-06-01",
        "exception": RuntimeError("GCS file not found"),
    }

    record_task_log(context, "FAILED")

    logged_entry = mock_log_pipeline_run.call_args[0][0]
    assert logged_entry.status == "FAILED"
    assert logged_entry.rows_read is None
    assert "GCS file not found" in logged_entry.error_message


@patch("src.observability.airflow_ops_logger.log_pipeline_run")
@patch("src.observability.airflow_ops_logger.bigquery.Client")
def test_record_task_log_handles_bigquery_error_gracefully(
    mock_bq_client, mock_log_pipeline_run
):
    """
    Kalau BigQuery gagal saat hitung row count, task tetap harus
    ter-log (dengan rows None), bukan crash callback Airflow.
    """
    mock_bq_client.side_effect = GoogleCloudError("connection timeout")

    mock_task_instance = MagicMock()
    mock_task_instance.task_id = "staging_layer.dbt_run_staging"
    mock_task_instance.start_date = datetime.now(timezone.utc)

    mock_dag = MagicMock()
    mock_dag.dag_id = "agungnugraha_batch_trip_pipeline"

    context = {
        "task_instance": mock_task_instance,
        "dag_run": MagicMock(),
        "dag": mock_dag,
        "run_id": "manual__2026-06-01",
        "data_interval_start": datetime(2026, 6, 1, tzinfo=timezone.utc),
    }

    record_task_log(context, "SUCCESS")

    logged_entry = mock_log_pipeline_run.call_args[0][0]
    assert logged_entry.rows_read is None


@patch("src.observability.airflow_ops_logger.record_task_log")
def test_task_success_callback_calls_record_with_success(mock_record):
    context = {"task_instance": MagicMock()}
    task_success_callback(context)
    mock_record.assert_called_once_with(context, "SUCCESS")


@patch("src.observability.airflow_ops_logger.record_task_log")
def test_task_failure_callback_calls_record_with_failed(mock_record):
    context = {"task_instance": MagicMock()}
    task_failure_callback(context)
    mock_record.assert_called_once_with(context, "FAILED")
