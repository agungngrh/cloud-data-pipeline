from google.cloud import bigquery

from config.settings import (
    BQ_TABLE_STREAM_CLEAN,
    BQ_TABLE_STREAM_QUARANTINE,
    GCP_PROJECT_ID,
)
from utils.logger import get_logger
from utils.schema import SCHEMA_CLEAN_TRIPS, SCHEMA_QUARANTINE_TRIPS

logger = get_logger(__name__)


def build_table(
    table_id: str, schema: list[bigquery.SchemaField], partition_field: str
) -> bigquery.Table:
    """
    Build a BigQuery table object with daily partitioning
    """
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    return table


def create_table_if_not_exists(client: bigquery.Client, table: bigquery.Table) -> None:
    """
    Safely create the table in BigQuery if it does not exist yet
    """
    created = client.create_table(table, exists_ok=True)
    logger.info(f"BigQuery table ready: {created.full_table_id}")


def setup_bigquery() -> None:
    """
    Set up the clean and quarantine BigQuery tables for streaming
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)

    clean_table = build_table(
        table_id=BQ_TABLE_STREAM_CLEAN,
        schema=SCHEMA_CLEAN_TRIPS,
        partition_field="event_time",
    )
    quarantine_table = build_table(
        table_id=BQ_TABLE_STREAM_QUARANTINE,
        schema=SCHEMA_QUARANTINE_TRIPS,
        partition_field="event_time",
    )

    create_table_if_not_exists(client, clean_table)
    create_table_if_not_exists(client, quarantine_table)


if __name__ == "__main__":
    setup_bigquery()
