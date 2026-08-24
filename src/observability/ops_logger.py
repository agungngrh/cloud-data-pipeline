from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from src.config.settings import OPS_TABLE_ID
from src.observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineRunLog:
    run_id: str
    pipeline_name: str
    pipeline_type: str
    start_time: datetime
    status: str
    task_id: str | None = None
    end_time: datetime | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_quarantined: int | None = None
    error_message: str | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        """
        Normalize values and populate default timestamps
        """
        self.pipeline_type = self.pipeline_type.upper()
        self.status = self.status.upper()

        now_utc = datetime.now(timezone.utc)

        if self.created_at is None:
            self.created_at = now_utc

        if self.end_time is None and self.status in {"SUCCESS", "FAILED"}:
            self.end_time = now_utc

    def to_dict(self) -> dict[str, Any]:
        """
        Convert log object into a BigQuery-compatible dictionary
        """
        data = asdict(self)

        for key in ["start_time", "end_time", "created_at"]:
            if isinstance(data.get(key), datetime):
                data[key] = data[key].isoformat()

        return data


def log_pipeline_run(log_data: PipelineRunLog) -> None:
    """
    Write pipeline operational metadata to BigQuery
    """
    try:
        client = bigquery.Client()

        errors = client.insert_rows_json(
            OPS_TABLE_ID,
            [log_data.to_dict()],
        )

        if errors:
            logger.error(
                "Failed to write pipeline ops log to BigQuery: %s",
                errors,
            )
            return

        logger.info(
            "Pipeline ops log recorded [%s | %s | %s]",
            log_data.pipeline_type,
            log_data.pipeline_name,
            log_data.status,
        )

    except GoogleCloudError as err:
        logger.error(
            "Failed to execute pipeline ops logging: %s",
            err,
        )
