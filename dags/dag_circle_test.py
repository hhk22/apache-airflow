
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="example_empty_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    t1 = EmptyOperator(task_id="t1", dag=dag)
    t2 = EmptyOperator(task_id="t2", dag=dag)
    t3 = EmptyOperator(task_id="t3", dag=dag)

    # t1 >> t2 >> t3 >> t1
    t1 >> t2 >> t3