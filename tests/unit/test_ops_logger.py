from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.observability.ops_logger import PipelineRunLog, log_pipeline_run


def test_pipeline_run_log_normalizes_case():
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="batch",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="success",
    )
    assert log.pipeline_type == "BATCH"
    assert log.status == "SUCCESS"


def test_pipeline_run_log_auto_sets_end_time_on_terminal_status():
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="SUCCESS",
    )
    assert log.end_time is not None


def test_pipeline_run_log_end_time_none_for_non_terminal_status():
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="RUNNING",
    )
    assert log.end_time is None


def test_pipeline_run_log_auto_sets_created_at():
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="SUCCESS",
    )
    assert log.created_at is not None


def test_pipeline_run_log_respects_explicit_end_time_and_created_at():
    fixed_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="SUCCESS",
        end_time=fixed_time,
        created_at=fixed_time,
    )
    assert log.end_time == fixed_time
    assert log.created_at == fixed_time


def test_to_dict_converts_datetime_to_isoformat():
    now = datetime.now(timezone.utc)
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=now,
        status="SUCCESS",
    )
    result = log.to_dict()
    assert isinstance(result["start_time"], str)
    assert isinstance(result["created_at"], str)
    assert isinstance(result["end_time"], str)


def test_to_dict_keeps_none_fields_as_none():
    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="RUNNING",
    )
    result = log.to_dict()
    assert result["end_time"] is None
    assert result["error_message"] is None


@patch("src.observability.ops_logger.bigquery.Client")
def test_log_pipeline_run_success(mock_client_class):
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = []
    mock_client_class.return_value = mock_client

    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="SUCCESS",
    )
    log_pipeline_run(log)

    mock_client.insert_rows_json.assert_called_once()


@patch("src.observability.ops_logger.bigquery.Client")
def test_log_pipeline_run_handles_insert_errors_gracefully(mock_client_class):
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = [{"error": "some BQ error"}]
    mock_client_class.return_value = mock_client

    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="SUCCESS",
    )
    log_pipeline_run(log)


@patch("src.observability.ops_logger.bigquery.Client")
def test_log_pipeline_run_handles_google_cloud_error_gracefully(mock_client_class):
    from google.cloud.exceptions import GoogleCloudError

    mock_client_class.side_effect = GoogleCloudError("connection failed")

    log = PipelineRunLog(
        run_id="run-1",
        pipeline_name="test_pipeline",
        pipeline_type="BATCH",
        task_id="task-1",
        start_time=datetime.now(timezone.utc),
        status="FAILED",
        error_message="upstream failure",
    )
    log_pipeline_run(log)
