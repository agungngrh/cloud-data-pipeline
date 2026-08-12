from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROFILE_DIR = BASE_DIR / "data" / "profile"
BATCH_PROFILING = PROFILE_DIR / "profile_output.json"

GREEN_TAXI_DATASET_URLS: tuple[str, ...] = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
)

DOWNLOAD_TIMEOUT_SECONDS = 30
HTTP_MAX_RETRIES = 3

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

RANDOM_SEED = 42
MAX_DISTANCE_POOL_SIZE = 10_000

PROFILED_COLUMNS: list[str] = [
    "VendorID",
    "payment_type",
    "trip_type",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "store_and_fwd_flag",
    "cbd_congestion_fee",
]

START_DATE = datetime(2026, 6, 1, tzinfo=timezone.utc)
END_DATE = datetime(2026, 7, 31, 23, 59, 59, tzinfo=timezone.utc)

INVALID_LOCATION_IDS: set[int] = {264, 265}

REQUIRED_FIELDS: list[str] = [
    "vendor_id",
    "rate_code_id",
    "passenger_count",
    "payment_type",
    "trip_type",
]

AMOUNT_COMPONENT_FIELDS: list[str] = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
]


PAYMENT_LABELS: dict[int, str] = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
}
