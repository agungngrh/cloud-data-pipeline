import shutil
from pathlib import Path
from urllib.parse import urlparse

import requests
from google.cloud import storage

from config.constants import (
    DOWNLOAD_TIMEOUT_SECONDS,
    GREEN_TAXI_DATASET_URLS,
    RAW_DATA_DIR,
)
from config.settings import GCP_PROJECT_ID, GCS_BUCKET
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_raw_file(bucket: storage.Bucket, url: str) -> None:
    """
    Ensure a single raw dataset file exists in local storage and GCS
    """
    filename = Path(urlparse(url).path).name
    blob_name = f"raw/{filename}"
    local_path = RAW_DATA_DIR / filename

    blob = bucket.blob(blob_name)

    if blob.exists():
        logger.info(f"GCS blob already exists: gs://{bucket.name}/{blob_name}")
        return

    if local_path.exists():
        logger.info(f"Local file already exists: {local_path}")
    else:
        logger.info(f"Downloading {filename}...")
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(
            url, timeout=DOWNLOAD_TIMEOUT_SECONDS, stream=True
        ) as response:
            response.raise_for_status()
            with local_path.open("wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)

    logger.info(f"Uploading {filename} to gs://{bucket.name}/{blob_name}")
    blob.upload_from_filename(local_path)


def run_ingestion() -> None:
    """
    Execute raw data ingestion for all configured dataset URLs
    """
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)

    try:
        for url in GREEN_TAXI_DATASET_URLS:
            ingest_raw_file(bucket, url)

        logger.info("Raw data ingestion completed successfully.")
    except Exception as err:
        logger.error(f"Raw data ingestion failed: {err}")
        raise


if __name__ == "__main__":
    run_ingestion()
