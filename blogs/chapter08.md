
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













