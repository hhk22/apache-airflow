
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow'
}

dag = DAG(
    "check_context",
    default_args=default_args,
    schedule_interval=None
)

def _print_context(execution_date, **context):
    print(execution_date)
    print(context.keys())

python_task = PythonOperator(
    task_id="python_task",
    python_callable=_print_context,
    dag=dag
)