import os

from dotenv import load_dotenv

load_dotenv()


def get_env(key: str, default: str | None = None, required: bool = True) -> str:
    """
    Retrieve environment variable with optional fallback and strict validation
    """
    value = os.environ.get(key, default)
    if required and (value is None or not value.strip()):
        raise ValueError(f"Missing required environment variable: '{key}' in .env")
    return value or ""


GCP_PROJECT_ID = get_env("GCP_PROJECT_ID")
STUDENT_ID = get_env("STUDENT_ID")
GCP_REGION = get_env("GCP_REGION")

GCS_BUCKET = get_env("GCS_BUCKET")
GCS_RAW_PATH = get_env("GCS_RAW_PATH")
GCS_STAGING_LOCATION = get_env("GCS_STAGING_LOCATION")
GCS_TEMP_LOCATION = get_env("GCS_TEMP_LOCATION")

PUBSUB_TOPIC = get_env("PUBSUB_TOPIC")
PUBSUB_SUBSCRIPTION = get_env("PUBSUB_SUBSCRIPTION")
SUBSCRIPTION_PATH = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION}"

BQ_DATASET_RAW = get_env("BQ_DATASET_RAW")
BQ_DATASET_STAGING = get_env("BQ_DATASET_STAGING")
BQ_DATASET_INTERMEDIATE = get_env("BQ_DATASET_INTERMEDIATE")
BQ_DATASET_MARTS = get_env("BQ_DATASET_MARTS")

DATAFLOW_STREAM_JOB_PREFIX = get_env(
    "DATAFLOW_STREAM_JOB_PREFIX", default=f"{STUDENT_ID}-stream", required=False
)

TABLE_TRIP_RAW = "raw_taxi_trip"
TABLE_TAXI_ZONE = "taxi_zone_lookup"
BQ_TABLE_TRIP_RAW = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{TABLE_TRIP_RAW}"

TABLE_STREAM_CLEAN = "stream_trips_clean"
TABLE_STREAM_QUARANTINE = "stream_trips_quarantine"
BQ_TABLE_STREAM_CLEAN = (
    f"{GCP_PROJECT_ID}.{BQ_DATASET_INTERMEDIATE}.{TABLE_STREAM_CLEAN}"
)
BQ_TABLE_STREAM_QUARANTINE = (
    f"{GCP_PROJECT_ID}.{BQ_DATASET_INTERMEDIATE}.{TABLE_STREAM_QUARANTINE}"
)
