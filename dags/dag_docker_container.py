from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='dag_http_sample',
    default_args=default_args,
    schedule_interval=None
) as dag:

    http_task = DockerOperator(
        task_id='http_get_task',
        image='http-operator-image:latest',
        command=[
            'python',
            '/app/http_task.py',
            '--output_path',
            '/data/{{ds}}.json'
        ],
        environment={"TARGET_URL": "https://jsonplaceholder.typicode.com/comments"},
        volumes=["/tmp/airflow/data:/data"],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
    )

    http_task
