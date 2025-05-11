
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

----- 

Github: https://github.com/hhk22/apache-airflow/tree/chapter09

-----

- Chapter 10. Airflow In Container

-----

현재까지 살펴 본 내용들을 보면, `Airflow` 에서는 다양한 오퍼레이터를 구성할 수 있었다. 그러나, 이러한 다양한 오퍼레이터를 사용할 때 가장 어려운 점은 각각의 오퍼레이터들이 **각기 다른 종속성을 요구한다는 것**이다. 

예를들어, 3개의 `Operator` 가 있다고 가정하자.

- `HttpOperator` : HTTP 요청을 수행하고, `request` 에 종속적. 
- `MySQLOperator` : MySQL 과 통신
- `CustomPythonOperator` : 추천코드 수행, `scikit-learn`, `pandas` 에 종속적.

그런데 문제는, `Airflow` 의 설정방식 때문에 이러한 모든 종속성을 `Airflow` 의 모든 구성요소에 동일한 환경이 제공되어야 한다는 점이다. (`스케줄러`, `워커`, `...`)

이러한 구조는 시스템이 커지면 커질수록 의존성 충돌문제의 가능성을 높인다. 

# Container Operator

100개의 `Operator`가 있다고 가정했을때, 가장 이상적인 구조는 어떤것일까? 100개의 `Operator` 가 다 다른 환경을 가지고 있다면 효과적일까? 그것은 오히려 100개의 환경을 관리하는것 또한 문제일것이다. 그렇다면? 몇개의 그룹을 나누는 것이다. 

- Generic Operator I
- Generic Operator II
- ... 

이처럼 대표 `Operator` 를 지정하고, 각각의 `Operator` 는 대표 `Generic Operator` 의 환경과 규칙을 따르게 하도록 하는 방법이다.  

`Airflow` 에서 이러한 독립적인 구조를 가저가게 하는 것으로 **Container Operator** 라는것이 있다. 

말그대로, `Docker Container` 위에서 해당 `Operator` 가 돌아가게 하는 구조이다. 

## HTTP Docker Operator

그럼 첫번째, HTTP 요청 관련 `Operator` 들을 실행하기 위한 `Docker Operator` 를 작성해보자. 

먼저 이러한 식으로 `Docker Operator` 를 실행할 것이다. 

```
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
```

`http_task.py` 를 실행하는 구조이고, 요청을 보낼 `URL` 은 환경변수로 `TARGET_URL` 이 설정되어 있어야 한다. 그리고 해당 URL에서는 응답을 `json` 타입으로 줘야하고, 그 데이터를 저장할 `output_path` 는 `args` 로 줘야 한다. 

그럼 먼저, `http_task.py` 를 작성해보자. 

```
import os
import requests
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--output_path", type=str, required=True)
args = parser.parse_args()

url = os.environ.get("TARGET_URL", "https://jsonplaceholder.typicode.com/comments")
response: requests.Response = requests.get(url)
response.raise_for_status()

data = response.json()
with open(args.output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
```

실행은 아래와 같이 하면 된다. 

```
export TARGET_URL=https://jsonplaceholder.typicode.com/comments
python http_task.py --output "/data/output.json"
```

그럼 해당 파일을 실행시킬 `Dockerfile` 을 작성해보자. 

```
FROM python:3.10-slim

RUN pip install requests
RUN mkdir /data

WORKDIR /app

COPY http_task.py /app/
```

그리고 `build` 는 아래와 같이 하면 된다. 

```
docker build -t http-operator-image:latest .
```

`/data` 경로에는 `host pc` 의 데이터폴더 경로를 마운트해서 해당 경로에 `output json` 파일들을 저장하는 용도로 사용할 것이다. 

이런식이면, `http` 요청을 보내는 `task` 들을 해당 `Operator` 를 사용하면 된다. 

그렇다면 결과적으로 이러한 `DAG`를 작성할 수 있다. 

```
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
```

이런식으로 다른 `Docker Operator` 도 구성할 수 있고, 각각의 `Docker Operator` 들은 격리된 환경에서 실행할 수 있다. 