import importlib.util
from datetime import datetime
from pathlib import Path

from airflow import DAG

DAG_FILE_PATH = Path("/opt/airflow/dags/batch_pipeline.py")


def _load_batch_pipeline_module():
    spec = importlib.util.spec_from_file_location(
        "batch_pipeline_under_test", DAG_FILE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


batch_pipeline = _load_batch_pipeline_module()
create_dbt_layer = batch_pipeline.create_dbt_layer


def test_create_dbt_layer_run_then_test_order():
    with DAG(dag_id="dummy_dag", start_date=datetime(2026, 1, 1), schedule=None):
        group = create_dbt_layer(layer_name="staging", dbt_vars="{}")

        run_task = group.get_child_by_label("dbt_run_staging")
        test_task = group.get_child_by_label("dbt_test_staging")

        assert test_task.task_id in [t.task_id for t in run_task.downstream_list]


def test_create_dbt_layer_task_ids_use_layer_name():
    with DAG(dag_id="dummy_dag", start_date=datetime(2026, 1, 1), schedule=None):
        group = create_dbt_layer(layer_name="marts", dbt_vars="{}")

        task_ids = [t.task_id for t in group.children.values()]

    assert any("dbt_run_marts" in tid for tid in task_ids)
    assert any("dbt_test_marts" in tid for tid in task_ids)


def test_create_dbt_layer_bash_command_uses_dbt_project_dir():
    with DAG(dag_id="dummy_dag", start_date=datetime(2026, 1, 1), schedule=None):
        group = create_dbt_layer(layer_name="staging", dbt_vars='{"key": "value"}')
        run_task = group.get_child_by_label("dbt_run_staging")

    assert "dbt run --select staging" in run_task.bash_command
    assert '{"key": "value"}' in run_task.bash_command
