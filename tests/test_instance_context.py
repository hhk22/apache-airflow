
import datetime

from airflow.models import BaseOperator, TaskInstance
from airflow.models.dag import DAG
from airflow.utils import timezone

class SampleDAG(BaseOperator):
    template_fields = ("_start_date", "_end_date")

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._end_date = end_date
    
    def execute(self, context):
        context["ti"].xcom_push(key="start_date", value=self._start_date)
        context["ti"].xcom_push(key="end_date", value=self._end_date)
        return context


def test_execute(test_dag: DAG):
    task = SampleDAG(
        task_id="test",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        dag=test_dag
    )

    task.run(
        start_date=test_dag.default_args["start_date"], 
        end_date=test_dag.default_args["end_date"],
        ignore_first_depends_on_past=True
    )

    expected_start_date = datetime.datetime(2025, 4, 5, tzinfo=timezone.utc)
    expected_end_date = datetime.datetime(2025, 4, 6, tzinfo=timezone.utc)
    assert task.start_date == expected_start_date
    assert task.end_date == expected_end_date