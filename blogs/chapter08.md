
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

-----

- Chapter 08. 커스텀 컴포넌트 빌드

----

 `Airflow` 에서는 다양한 오퍼레이트들을 제공한다. 우리는 그러한 오퍼레이트들을 사용해서 간단하게 워크플로우를 만들 수 있지만, 필요에 따라서는 사용자가 직접 오퍼레이터를 재정의해서 사용할 수 있다. 

 하나씩 커스텀오퍼레이터를 어떻게 작성하는지 알아보자. 

# Custom Python Operator

이번장에서는 `영화 평점 API` --> `신규 평점 데이터 가져오기` --> `인기 영화 랭킹(일간)` --> `App` 의 파이프라인을 작성해보자. 

먼저, 영화 평점 API를 만들어보자. 

코드는 따로 작성하지 않고, 책에 있는 코드를 환경설정만 조금 바꾸고 거의 카피했다. 

`docker-compose.yml` 에서 해당부분을 추가해줬고,

```
movielens:
    build: docker/movielens-api
    image: manning-airflow/movielens-api
    ports:
        - "5000:5000"
    environment:
        API_USER: airflow
        API_PASSWORD: airflow
```

해당 컨테이너가 올라오게 되면, 

아래의 주소로 영화 리뷰를 얻기 위한 요청들을 보낼 수 있다. 

`http://localhost:5000/ratings`

`http://localhost:5000/ratings?offset=100`

`http://localhost:5000/ratings?limit=1000`

`http://localhost:5000/ratings?start_date=2019-01-01&end_date=2019-01-02`

이를 기반으로, `API` 를 작성해보자. 

먼저, 아래와 같은 함수들을 만들어야 한다. 

- `session` 을 가져오는 함수. 

- `session` 과 `params` 를 통해서 `response` 를 받아오는 함수

- `date` 를 기준으로 `rating` 들을 받아오는 함수. 

그러고 나서, 최종적으로 아래의 두 함수를 만들고 `PythonOperator` 에 연결시켜야 한다. 

- `fetch_ratings` : 위의 3가지 함수들을 통해서 `response` 를 받고, `json` 파일을 생성시킨다. 

- `rank_movies` : `json` 파일을 가지고서 `pandas` 를 이용해 영화의 랭킹을 `csv` 파일을 생성시킨다. 

코드는 다음과 같다. 

```
# dags/dag_python_operator.py

import requests
import os
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from custom.ranking import rank_movies_by_rating

import json
import logging
import pandas as pd

MOVIELENS_HOST = os.environ.get("MOVIELENS_HOST", "movielens")
MOVIELENS_SCHEMA = os.environ.get("MOVIELENS_SCHEMA", "http")
MOVIELENS_PORT = os.environ.get("MOVIELENS_PORT", "5000")

MOVIELENS_USER = "airflow"
MOVIELENS_PASSWORD = "airflow"

def _get_ratings(start_date, end_date, batch_size=100):
    session, base_url = _get_session()

    yield from _get_with_pagination(
        session=session,
        url=base_url + "/ratings",
        params={"start_date": start_date, "end_date": end_date},
        batch_size=batch_size,
    )

def _get_session():
    session = requests.Session()
    session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)

    schema = MOVIELENS_SCHEMA
    host = MOVIELENS_HOST
    port = MOVIELENS_PORT

    base_url = f"{schema}://{host}:{port}"
    return session, base_url

def _get_with_pagination(session, url, params, batch_size=100):
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url, params={**params, **{"offset": offset, "limit": batch_size}}
        )
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"]

        offset += batch_size
        total = response_json["total"]

with DAG(
    dag_id="custom_python_operator_example",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 3),
    schedule_interval="@daily",
) as dag:
    
    def _fetch_ratings(templates_dict, batch_size=1000, **_):
        logger = logging.getLogger(__name__)

        start_date = templates_dict["start_date"]
        end_date = templates_dict["end_date"]
        output_path = templates_dict["output_path"]

        logger.info(f"Fetching ratings for {start_date} to {end_date}")
        ratings = list(
            _get_ratings(
                start_date=start_date, end_date=end_date, batch_size=batch_size
            )
        )
        logger.info(f"Fetched {len(ratings)} ratings")

        logger.info(f"Writing ratings to {output_path}")

        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w") as file_:
            json.dump(ratings, fp=file_)


    fetch_ratings = PythonOperator(
        task_id="fetch_ratings",
        python_callable=_fetch_ratings,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/ratings/{{ds}}.json",
        },
    )

    def _rank_movies(templates_dict, min_ratings=2, **_):
        input_path = templates_dict["input_path"]
        output_path = templates_dict["output_path"]

        ratings = pd.read_json(input_path)
        ranking = rank_movies_by_rating(ratings, min_ratings=min_ratings)

        # Make sure output directory exists.
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        ranking.to_csv(output_path, index=True)

    rank_movies = PythonOperator(
        task_id="rank_movies",
        python_callable=_rank_movies,
        templates_dict={
            "input_path": "/data/python/ratings/{{ds}}.json",
            "output_path": "/data/python/rankings/{{ds}}.csv",
        },
    )

    fetch_ratings >> rank_movies

```

이부분(`from custom.ranking import rank_movies_by_rating
`)에 대한 함수는 아래와 같이 구현할 수 있다. 

```
import pandas as pd

def rank_movies_by_rating(ratings, min_ratings=2):
    ranking = (
        ratings.groupby("movieId")
        .agg(
            avg_rating=pd.NamedAgg(column="rating", aggfunc="mean"),
            num_ratings=pd.NamedAgg(column="userId", aggfunc="nunique"),
        )
        .loc[lambda df: df["num_ratings"] > min_ratings]
        .sort_values(["avg_rating", "num_ratings"], ascending=False)
    )
    return ranking
```

근데 이러한 코드들이 에어프로우에서 제공하는 여러가지 기능을 통해서 좀 더 쉽게 구현할 수 있습니다. 그것에 대해서 알아봅시다. 

# Custom Hook













