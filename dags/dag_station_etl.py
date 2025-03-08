from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from station_etl import get_station_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

dag = DAG(
    'dag_html_etl', 
    default_args=default_args, 
    schedule_interval='@monthly',
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=get_station_data,
    op_kwargs={"datetime": "{{ds}}"},
    dag=dag
)
