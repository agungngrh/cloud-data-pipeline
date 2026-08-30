import pytest
from airflow.models import DagBag

from src.config.settings import settings
from src.observability.airflow_ops_logger import (
    task_failure_callback,
    task_success_callback,
)

DAG_ID = "agungnugraha_batch_trip_pipeline"


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    assert dag is not None
    assert len(dag.tasks) > 0


def test_dag_schedule_and_catchup(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.schedule_interval == "@monthly"


def test_all_task_groups_present(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    task_ids = [t.task_id for t in dag.tasks]
    for group_name in [
        "ingestion_layer",
        "staging_layer",
        "intermediate_layer",
        "marts_layer",
    ]:
        assert any(
            tid.startswith(f"{group_name}.") for tid in task_ids
        ), f"{group_name} tidak ditemukan"


def test_ingestion_task_order(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    check = dag.get_task("ingestion_layer.check_trip_file")
    create = dag.get_task("ingestion_layer.create_raw_trip_table")
    delete = dag.get_task("ingestion_layer.delete_trip_period")
    load = dag.get_task("ingestion_layer.load_trip_data_raw")
    assert create.task_id in [t.task_id for t in check.downstream_list]
    assert delete.task_id in [t.task_id for t in create.downstream_list]
    assert load.task_id in [t.task_id for t in delete.downstream_list]


def test_load_trip_data_raw_correct_bucket(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    task = dag.get_task("ingestion_layer.load_trip_data_raw")
    assert task.bucket == settings.gcs_bucket
    assert "." not in task.bucket, (
        "bucket name tidak boleh mengandung titik "
        "(indikasi BQ table ID 'project.dataset.table' salah masuk)"
    )


def test_create_raw_trip_table_uses_correct_table(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    task = dag.get_task("ingestion_layer.create_raw_trip_table")
    query = task.configuration["query"]["query"]
    assert settings.bq_table_trip_raw in query


def test_default_args_callbacks_attached(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    assert dag.default_args["on_success_callback"] == task_success_callback
    assert dag.default_args["on_failure_callback"] == task_failure_callback


def test_dbt_layer_dependency_chain(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)

    load = dag.get_task("ingestion_layer.load_trip_data_raw")
    staging_run = dag.get_task("staging_layer.dbt_run_staging")
    assert staging_run.task_id in [t.task_id for t in load.downstream_list]

    staging_test = dag.get_task("staging_layer.dbt_test_staging")
    intermediate_run = dag.get_task("intermediate_layer.dbt_run_intermediate")
    assert intermediate_run.task_id in [t.task_id for t in staging_test.downstream_list]

    intermediate_test = dag.get_task("intermediate_layer.dbt_test_intermediate")
    marts_run = dag.get_task("marts_layer.dbt_run_marts")
    assert marts_run.task_id in [t.task_id for t in intermediate_test.downstream_list]


def test_all_dbt_layers_have_run_and_test_tasks(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_ID)
    task_ids = [t.task_id for t in dag.tasks]
    for layer in ["staging", "intermediate", "marts"]:
        assert f"{layer}_layer.dbt_run_{layer}" in task_ids
        assert f"{layer}_layer.dbt_test_{layer}" in task_ids
