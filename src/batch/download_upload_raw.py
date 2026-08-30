import shutil
from pathlib import Path
from urllib.parse import urlparse

import requests
from google.cloud import storage

from src.config.constants import (
    DOWNLOAD_TIMEOUT_SECONDS,
    GREEN_TAXI_DATASET_URLS,
    RAW_DATA_DIR,
)
from src.config.settings import settings
from src.observability.logger import get_logger

logger = get_logger(__name__)


def ingest_raw_file(bucket: storage.Bucket, url: str) -> None:
    """
    Ensure a raw dataset file exists locally before uploading it to GCS.
    """
    filename = Path(urlparse(url).path).name
    local_path = RAW_DATA_DIR / filename
    blob_name = f"raw/{filename}"

    if local_path.exists():
        logger.info("Local file already exists: %s", local_path)
    else:
        logger.info("Downloading %s...", filename)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(
            url,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
            stream=True,
        ) as response:
            response.raise_for_status()

            with local_path.open("wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)

        logger.info("Downloaded %s to %s", filename, local_path)

    blob = bucket.blob(blob_name)

    if blob.exists():
        logger.info(
            "GCS blob already exists: gs://%s/%s",
            bucket.name,
            blob_name,
        )
        return

    logger.info("Uploading %s to gs://%s/%s", filename, bucket.name, blob_name)
    blob.upload_from_filename(local_path)

    logger.info("Uploaded %s successfully", filename)


def run_ingestion() -> None:
    """
    Execute raw data ingestion for all configured dataset URLs.
    """
    client = storage.Client(project=settings.gcp_project_id)
    bucket = client.bucket(settings.gcs_bucket)

    try:
        for url in GREEN_TAXI_DATASET_URLS:
            ingest_raw_file(bucket, url)

        logger.info("Raw data ingestion completed successfully.")
    except Exception as err:
        logger.error("Raw data ingestion failed: %s", err)
        raise


if __name__ == "__main__":
    run_ingestion()
