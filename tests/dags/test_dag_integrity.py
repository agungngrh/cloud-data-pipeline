import pytest
from airflow.models import DagBag

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
    # TaskGroup prefix task_id dengan nama group, mis. "ingestion_layer.check_trip_file"
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
