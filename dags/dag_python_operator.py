
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

# MOVIELENS_USER = os.environ["MOVIELENS_USER"]
MOVIELENS_USER = "airflow"
# MOVIELENS_PASSWORD = os.environ["MOVIELENS_PASSWORD"]
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




