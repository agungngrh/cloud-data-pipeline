import json
from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from src.config.settings import settings
from src.observability.airflow_ops_logger import (
    task_failure_callback,
    task_success_callback,
)

DEFAULT_ARGS = {
    "owner": "agung_nugraha",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
    "on_success_callback": task_success_callback,
    "on_failure_callback": task_failure_callback,
}


def create_dbt_layer(layer_name: str, dbt_vars: str) -> TaskGroup:
    """
    Create a standardized dbt run and test TaskGroup.
    """
    with TaskGroup(
        group_id=f"{layer_name}_layer",
        tooltip=f"Run and test {layer_name} dbt models",
    ) as group:

        run = BashOperator(
            task_id=f"dbt_run_{layer_name}",
            bash_command=(
                f"cd {settings.dbt_project_dir} && "
                f"dbt run --select {layer_name} --vars '{dbt_vars}'"
            ),
        )

        test = BashOperator(
            task_id=f"dbt_test_{layer_name}",
            bash_command=(
                f"cd {settings.dbt_project_dir} && "
                f"dbt test --select {layer_name} --vars '{dbt_vars}'"
            ),
        )

        run >> test

    return group


with DAG(
    dag_id="agungnugraha_batch_trip_pipeline",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 5, 31, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gcs", "bigquery", "dbt", "ops"],
) as dag:

    dbt_vars = json.dumps(
        {"reporting_year_month": "{{ data_interval_start.strftime('%Y-%m') }}"}
    )

    with TaskGroup(
        group_id="ingestion_layer",
        tooltip="Ingest monthly trip data from GCS to BigQuery",
    ) as ingestion:

        check_trip_file = GCSObjectExistenceSensor(
            task_id="check_trip_file",
            bucket=settings.gcs_bucket,
            object=(
                "raw/green_tripdata_"
                '{{ data_interval_start.strftime("%Y-%m") }}.parquet'
            ),
            google_cloud_conn_id="google_cloud_default",
            timeout=300,
            poke_interval=60,
            mode="reschedule",
        )

        create_raw_trip_table = BigQueryInsertJobOperator(
            task_id="create_raw_trip_table",
            configuration={
                "query": {
                    "query": f"""
                        CREATE TABLE IF NOT EXISTS `{settings.bq_table_trip_raw}`
                        (
                            VendorID INT64,
                            lpep_pickup_datetime TIMESTAMP,
                            lpep_dropoff_datetime TIMESTAMP,
                            store_and_fwd_flag STRING,
                            RatecodeID INT64,
                            PULocationID INT64,
                            DOLocationID INT64,
                            passenger_count INT64,
                            trip_distance FLOAT64,
                            fare_amount FLOAT64,
                            extra FLOAT64,
                            mta_tax FLOAT64,
                            tip_amount FLOAT64,
                            tolls_amount FLOAT64,
                            ehail_fee FLOAT64,
                            improvement_surcharge FLOAT64,
                            total_amount FLOAT64,
                            payment_type INT64,
                            trip_type INT64,
                            congestion_surcharge FLOAT64,
                            cbd_congestion_fee FLOAT64
                        )
                        PARTITION BY DATE_TRUNC(lpep_pickup_datetime, MONTH)
                        CLUSTER BY PULocationID, DOLocationID;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="google_cloud_default",
        )

        delete_trip_period = BigQueryInsertJobOperator(
            task_id="delete_trip_period",
            configuration={
                "query": {
                    "query": (
                        f"DELETE FROM `{settings.bq_table_trip_raw}`\n"
                        "WHERE lpep_pickup_datetime >= TIMESTAMP('{{ data_interval_start }}')\n"
                        "  AND lpep_pickup_datetime < TIMESTAMP('{{ data_interval_end }}')"
                    ),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="google_cloud_default",
        )

        load_trip_data_raw = GCSToBigQueryOperator(
            task_id="load_trip_data_raw",
            bucket=settings.gcs_bucket,
            source_objects=[
                "raw/green_tripdata_"
                '{{ data_interval_start.strftime("%Y-%m") }}.parquet'
            ],
            destination_project_dataset_table=settings.bq_table_trip_raw,
            source_format="PARQUET",
            write_disposition="WRITE_APPEND",
            autodetect=True,
            gcp_conn_id="google_cloud_default",
        )

        (
            check_trip_file
            >> create_raw_trip_table
            >> delete_trip_period
            >> load_trip_data_raw
        )

    staging = create_dbt_layer(layer_name="staging", dbt_vars=dbt_vars)
    intermediate = create_dbt_layer(layer_name="intermediate", dbt_vars=dbt_vars)
    marts = create_dbt_layer(layer_name="marts", dbt_vars=dbt_vars)

    ingestion >> staging >> intermediate >> marts
