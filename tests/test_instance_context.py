
import datetime
import pytest
import time

from airflow.models import BaseOperator, TaskInstance
from airflow.models.dag import DAG
from airflow.utils.types import DagRunType
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils import timezone

class SampleOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date")

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._end_date = end_date
    
    def execute(self, context):
        context["ti"].xcom_push(key="start_date", value=self._start_date)
        context["ti"].xcom_push(key="end_date", value=self._end_date)
        return context

@pytest.fixture
def dag():
    with DAG(
        dag_id="test_dag_id",
        schedule=None,
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2025, 4, 5),
            "end_date": datetime.datetime(2025, 4, 6)
        }
    ) as dag:
        
        SampleOperator(
            task_id="test_task_id",
            start_date="{{ prev_ds }}",
            end_date="{{ ds }}"
        )
    
    return dag


def test_execute(dag: DAG):
    run_id = f"test_run_id_{datetime.datetime.now().strftime('%y-%m-%d:%H-%M-%s')}"
    dagrun = dag.create_dagrun(
        data_interval=(
            dag.default_args["start_date"],
            dag.default_args["end_date"]
        ),
        run_id=run_id,
        start_date=dag.default_args["start_date"],
        state=DagRunState.RUNNING,
    )

    ti = dagrun.get_task_instance(task_id="test_task_id")
    assert ti is not None
    # ti is None

    # ti = task.get_task_instances(
    #     start_date=test_dag.default_args["start_date"], 
    #     end_date=test_dag.default_args["end_date"],
    # )
    # ti.task = test_dag.get_task(task_id="test")
    # print(ti, "sample")

    # task.run(
    #     start_date=test_dag.default_args["start_date"], 
    #     end_date=test_dag.default_args["end_date"],
    #     ignore_first_depends_on_past=True
    # )

    # expected_start_date = datetime.datetime(2025, 4, 5, tzinfo=timezone.utc)
    # expected_end_date = datetime.datetime(2025, 4, 6, tzinfo=timezone.utc)
    # assert task.start_date == expected_start_date
    # assert task.end_date == expected_end_date