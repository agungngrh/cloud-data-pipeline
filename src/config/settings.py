from pydantic import computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    gcp_project_id: str
    student_id: str
    gcp_region: str
    gcs_bucket: str
    gcs_raw_path: str
    gcs_staging_location: str
    gcs_temp_location: str
    pubsub_topic: str
    pubsub_subscription: str
    bq_dataset_raw: str
    bq_dataset_staging: str
    bq_dataset_intermediate: str
    bq_dataset_marts: str
    bq_dataset_ops: str
    dataflow_stream_job_prefix: str | None = None

    dbt_project_dir: str = "/opt/airflow/dbt"
    dbt_profiles_dir: str = "/opt/airflow/dbt"

    streaming_requirements_file: str = "./src/streaming/requirements.txt"

    @model_validator(mode="after")
    def _apply_defaults(self) -> "Settings":
        if not self.dataflow_stream_job_prefix:
            self.dataflow_stream_job_prefix = f"{self.student_id}-stream"
        return self

    @computed_field
    @property
    def subscription_path(self) -> str:
        return (
            f"projects/{self.gcp_project_id}/subscriptions/{self.pubsub_subscription}"
        )

    @computed_field
    @property
    def bq_table_trip_raw(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_dataset_raw}.raw_taxi_trip"

    @computed_field
    @property
    def bq_table_stream_clean(self) -> str:
        return (
            f"{self.gcp_project_id}.{self.bq_dataset_intermediate}"
            ".int_stream_trips_clean"
        )

    @computed_field
    @property
    def bq_table_stream_quarantine(self) -> str:
        return (
            f"{self.gcp_project_id}.{self.bq_dataset_intermediate}"
            ".int_stream_trips_quarantine"
        )

    @computed_field
    @property
    def ops_table_id(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_dataset_ops}.pipeline_run_log"


settings = Settings()  # type: ignore[call-arg]
