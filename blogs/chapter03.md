
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

제가 자주출근하는 2호선 출근 데이터를 가지고서 코드를 작성했습니다. 

------

- Chapter 03. Airflow 스케줄링 

------

## 데이터 증분 처리

데이터 ETL파이프라인을 설계할때, 전체데이터를 가져와서 ETL파이프라인을 설계하는것은 효과적이지 못하다. 그에 대한 대안으로 데이터를 순차적으로 가져와서 파이프라인을 태우는 것이다. 

이러한 것을 `데이터 증분 처리` 라고 한다.  

예를들어, DAG를 설계할때, 30일간의 전체데이터를 ETL파이프라인을 태우는것이 아니라, 특정 날짜의 데이터만 파이프라인을 태우도록 설계하는것이다. 

`Airflow` 에서는 Operator에서 동적으로 시간을 참조하여 그 변수를 받아서 처리할 수 있는 기능이 있다. 

특히, `Airflow` 에서는 `Operator` 에서 템플릿변수로 사용할 수 있는데 아래와 같이 사용할 수 있다. 

```
python_task = PythonOperator(
    task_id='python_task',
    python_callable=get_station_data,
    op_kwargs={"datetime": "{{ds}}"},
    dag=dag
)
```

여기서, `{{ds}}` 변수를 템플릿변수로 사용할 수 있고, 이건 `Airflow` 에서 제공해주는 변수이다. 이외에도 `Airflow` 에서는 다양한 템플릿변수들을 제공한다. [여기](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html) 에서 확인할 수 있다.

저런식으로 작성하게되면, `python_callable` 함수에 템플레이팅된 변수값을 사용할 수 있다. 실제 사용하는 모습

```
def get_station_data(datetime: str):
    print(datetime)
    >>> "2025-03-08"
```

## 원자성과 멱등성

### 원자성

원자성은 하나의 작업에 여러가지 결과물을 만들어내면 안된다 라는 개념이다.  

예를들어, 이벤트데이터를 가져와서 상위 10개의 데이터를 csv파일에 쓰고, 그것을 고객들에게 email을 보내는 작업이 있다고 하자. 

여기서 csv파일에 쓰는것(작업1) 과 그 결과물을 고객들에게 보내는것(작업2) 가 있다. 

단 하나의 작업이 아니기때문에, 작업2에서 실패했을때(이메일 보내기를 실패했을때) 라면, 

이메일을 보내지 않았는데 csv파일은 이미 써져있는 혼돈의 상황이 생길 수 있다. 

원자성을 지켜, 작업1과 작업2를 나눠놓으면 작업1을 성공했을것이고, 작업2는 실패했을것이고, 작업2만 다시 실행시킬 수 있어 어떤 작업이 실패했고, 결과물은 어떻게 보장될것인지가 더 명확해진다. 

이처럼, 하나의 작업에는 하나의 결과물만 나올 수 있도록 원자성을 지켜, 코드를 작성하는 것이 중요하다. 

### 멱등성

멱등성은 언제실행하더라도 똑같은 결과물을 만들어 내야 한다. 이다.

코드를 작성해보자. 위의 `get_station_data` 를 이어서 작성해 보게되면, 2호선 지하철데이터를 계산해서 파일을 쓰는 간단한 샘플 코드를 작성해보자. 

```
stations = [
    "사당", "방배", "서초",
    "교대", "강남", "역삼", "선릉"
]

directory = "/opt/airflow/plugins"

def get_station_data(datetime: str):
    print('datetime', datetime)
    year, month, _ = datetime.split("-")

    url = "http://openapi.seoul.go.kr:8088/<auth_key>/xml/CardSubwayTime/1/5"

    result = {}
    for station in stations:
        request_url = f"{url}/{year}{month}/2호선/{station}"
        print(request_url)
        html = requests.get(request_url)
        bs = BeautifulSoup(html.content)

        tags: list[Tag] = bs.find_all(lambda tag: tag.name.startswith("hr_"))
        tot = 0
        for tag in tags:
            tot += int(tag.text)
        
        result[station] = tot
        
    with open(f"{directory}/{datetime}.json", "w") as f:
        f.write(result)
```

여기선, 작업을 돌릴때마다 해당 작업의 `datetime` 에 따라서 `json` 파일을 쓰고 있다. 만약있다면, 덮어쓸것이다. 그래서 결국엔 언제 어떠한 상황에서 돌리더라도 똑같은 결과물을 작성한다. 

근데 만약에 작업을 돌릴때마다 해당폴더에 파일을 추가하는것이라면, 작업을 돌릴때마다 파일이 계속 추가되므로, 멱등성이 보장되지 않는다. 

이처럼, 작업을 작성할때는, <b>원자성</b>, <b>멱등성</b> 을 보장해서 작성하는것을 유념해야 한다. 

---- 

실제코드는 [여기](https://github.com/hhk22/apache-airflow/tree/chapter03)서 확인할 수 있다. 

----

References

- https://www.yes24.com/Product/Goods/107878326

- https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html 

- https://data.seoul.go.kr/

----
