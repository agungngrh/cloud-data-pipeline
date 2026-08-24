from datetime import datetime

from airflow import DAG
from airflow.dags.batch_pipeline import create_dbt_layer


def test_create_dbt_layer_run_then_test_order():
    with DAG(dag_id="dummy_dag", start_date=datetime(2026, 1, 1), schedule=None):
        group = create_dbt_layer(layer_name="staging", dbt_vars="{}")

        run_task = group.get_child_by_label("dbt_run_staging")
        test_task = group.get_child_by_label("dbt_test_staging")

        assert test_task.task_id in [t.task_id for t in run_task.downstream_list]
