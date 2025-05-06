from airflow.models.baseoperator import BaseOperator
import datetime

import pendulum
import pytest

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

DATA_INTERVAL_START = pendulum.now("UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "my_custom_operator_dag"
TEST_TASK_ID = "my_custom_operator_task"


class SampleOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date")

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._end_date = end_date
    
    def execute(self, context):
        print(context)
        context["ti"].xcom_push(key="start_date", value=self._start_date)
        context["ti"].xcom_push(key="end_date", value=self._end_date)
        return "context"


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        SampleOperator(
            start_date="{{ prev_ds }}",
            end_date="{{ next_ds  }}",
            task_id=TEST_TASK_ID
        )
    return dag


def test_my_custom_operator_execute_no_trigger(dag: DAG):
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(
        task_id=TEST_TASK_ID
    )

    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.render_templates()
    ti.run(ignore_ti_state=True)
    
    start_date = ti.xcom_pull(task_ids=TEST_TASK_ID, key="start_date")
    end_date = ti.xcom_pull(task_ids=TEST_TASK_ID, key="end_date")

    assert ti.state == TaskInstanceState.SUCCESS
    assert start_date == DATA_INTERVAL_START.strftime("%Y-%m-%d")
    assert end_date == DATA_INTERVAL_END.strftime("%Y-%m-%d")
