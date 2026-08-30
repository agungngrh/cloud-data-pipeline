from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.streaming.monitoring import get_written_counts, log_operational_run


def test_get_written_counts_returns_correct_tuple():
    mock_client = MagicMock()
    mock_row = MagicMock()
    mock_row.rows_written = 42
    mock_row.rows_quarantined = 3

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([mock_row])
    mock_client.query.return_value = mock_query_job

    rows_written, rows_quarantined = get_written_counts(
        client=mock_client,
        start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 6, 2, tzinfo=timezone.utc),
    )

    assert rows_written == 42
    assert rows_quarantined == 3


def test_get_written_counts_casts_to_int():
    mock_client = MagicMock()
    mock_row = MagicMock()
    mock_row.rows_written = "10"
    mock_row.rows_quarantined = "0"

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([mock_row])
    mock_client.query.return_value = mock_query_job

    rows_written, rows_quarantined = get_written_counts(
        client=mock_client,
        start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 6, 2, tzinfo=timezone.utc),
    )

    assert rows_written == 10
    assert rows_quarantined == 0
    assert isinstance(rows_written, int)


def test_get_written_counts_passes_query_parameters():
    mock_client = MagicMock()
    mock_row = MagicMock()
    mock_row.rows_written = 0
    mock_row.rows_quarantined = 0

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([mock_row])
    mock_client.query.return_value = mock_query_job

    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 2, tzinfo=timezone.utc)

    get_written_counts(client=mock_client, start_time=start, end_time=end)

    _, kwargs = mock_client.query.call_args
    job_config = kwargs["job_config"]
    param_values = {p.name: p.value for p in job_config.query_parameters}

    assert param_values["start_time"] == start
    assert param_values["end_time"] == end


@patch("src.streaming.monitoring.log_pipeline_run")
def test_log_operational_run_builds_correct_pipeline_run_log(mock_log_pipeline_run):
    log_operational_run(
        run_id="stream_abc123",
        start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 6, 1, 1, tzinfo=timezone.utc),
        status="SUCCESS",
        error_message=None,
        rows_read=100,
        rows_written=95,
        rows_quarantined=5,
    )

    mock_log_pipeline_run.assert_called_once()
    logged_entry = mock_log_pipeline_run.call_args[0][0]

    assert logged_entry.run_id == "stream_abc123"
    assert logged_entry.pipeline_type == "STREAMING"
    assert logged_entry.status == "SUCCESS"
    assert logged_entry.rows_read == 100
    assert logged_entry.rows_written == 95
    assert logged_entry.rows_quarantined == 5


@patch("src.streaming.monitoring.log_pipeline_run")
def test_log_operational_run_passes_error_message_on_failure(mock_log_pipeline_run):
    log_operational_run(
        run_id="stream_abc123",
        start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 6, 1, 1, tzinfo=timezone.utc),
        status="FAILED",
        error_message="Beam runtime exception",
        rows_read=0,
        rows_written=0,
        rows_quarantined=0,
    )

    logged_entry = mock_log_pipeline_run.call_args[0][0]
    assert logged_entry.error_message == "Beam runtime exception"
