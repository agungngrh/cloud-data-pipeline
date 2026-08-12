from google.api_core.exceptions import Conflict
from google.cloud import storage

from config.settings import GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET
from utils.logger import get_logger

logger = get_logger(__name__)


def create_bucket() -> None:
    """
    Create the project Google Cloud Storage bucket
    """
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage.Bucket(client=client, name=GCS_BUCKET)

    try:
        client.create_bucket(bucket, location=GCP_REGION)
        logger.info(f"GCS bucket created: gs://{GCS_BUCKET}")
    except Conflict:
        logger.info(f"GCS bucket ready: gs://{GCS_BUCKET}")


if __name__ == "__main__":
    create_bucket()
