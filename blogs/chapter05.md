
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

제가 자주출근하는 2호선 출근 데이터를 가지고서 코드를 작성했습니다. 

-----

- Chapter 05. Airflow Task간 의존성

----

이전 글에서는 선형의존성으로 Task들이 연결되어 있었다. 이런식으로

```
task1 >> task2 >> task3
```

Airflow는 이러한 선형의존성의 관계말고도 팬인/팬아웃(Fan-in/Fan-out) 의존성도 지원한다. 

팬인/팬아웃 관계를 그림으로 나타내면 다음과 같다. 

![](https://velog.velcdn.com/images/khhh9401/post/45b5b8ce-587c-4549-b152-54445b21c47a/image.png)
`<그림-1>`

코드로 나타내면 다음과 같다. 

```
start >> [fetch_weather, fetch_sales]

fetch_weather >> clean_weather
fetch_sales >> clean_sales

[clean_weather, clean_sales] >> join_datasets

join_datasets >> train_model >> deploy_model
```

## 브랜치

만약 Task를 실행하는데, 특정조건에서는 A동작을 하게하고, 특정조건에서는 B동작을 하게하려한다면 어떻게 해야할까?

예를들어, `clean_sales`, `fetch_sales` 에서 특정날짜 기준으로 `old`, `new` 로 나눠서 실행하게 하고 싶다면 이런식으로 코드를 작성할 수 있다. 

```
def _clean_sales(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        _clean_sales_old(**context)
    else:
        _clean_sales_new(**context)

clean_sales_data = PythonOperator(
    task_id="clean_sales",
    python_callable=_clean_sales
)

# 마찬가지로, 

def _fetch_sales(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        _fetch_sales_old(**context)
    else:
        _fetch_sales_new(**context)

...
```

이런식으로 `Task함수` 내에서 분기처리하는것은 코드로 작성했기 때문에 DAG의 구조를 변경하지 않고도 코드만 변경하면 분기처리가 자유롭다는 점에서 유연성이 있다고 할 수 있다. 

하지만 이런식으로 처리하는것은 어느정도 한계가 있다. 책에서는 이러한 방식의 단점을 크게 두가지로 뽑고 있다. 

1. 서로다른 테스크일 경우, 분기처리가 어려워질 수 있다. 

예를들어, 기존시스템과 새로운시스템이 이런식으로 구성되어 있다면,

```
# 기존 시스템
압축된 판매 데이터 가져오기 >> 압축 풀기 >> 고객정보 추가 >> 판매데이터 정제

# 새로운 시스템
판매데이터 가져오기(새로운 API) >> 판매데이터 정제
```

이런식으로 아예 기존시스템과 새로운시스템의 Task구조가 크게 다르다면, 각 코드에서 분기처리를 어떻게 추가해줘야할지, 또는 코드로 구성했다 하더라도 일관성이 깨져 유지보수가 어려워 질것이다. (규모가 크다면 더더욱)

2. `Task Pipeline` 을 보고 해당 `job` 이 어떠한 조건으로 동작했는지 파악하기 어렵다. 

`<그림-1>` 을 봤을때, 만약 Task파이프라인을 돌린다면 어떠한 시스템, 어떠한 조건으로 동작했는지 파악하기가 어렵다. 왜냐하면 어떠한 조건에서 돌리든 모두 동일한 `DAG` 구조로 실행이 되고 코드에서 분기처리가 되기 때문이다. 결국 `log` 까지 확인해봐야 알 수 있고, `log` 작성에도 신경을 써야 할것이다. 

### Branch Operator

분기처리의 대안으로 Airflow에서 지원하는 `Branch Operator` 가 대안이 될 수 있다. 

`Branch Operator` 의 예시를 코드로 작성해보면 다음과 같다. 

```
def _pick_erp_system(**context):
    number = random.choice([1, 2])
    print(f"number: {number}")
    if number == 1:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"

fetch_sales_old = PythonOperator(
    task_id="fetch_sales_old",
    python_callable=lambda: print("Task A 실행")
)

fetch_sales_new = PythonOperator(
    task_id="fetch_sales_new",
    python_callable=lambda: print("Task B 실행")
)

with DAG("branch_operator_example", schedule_interval=None) as dag:
    pick_erp_system=BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=_pick_erp_system
    )

    pick_erp_system >> [fetch_sales_old, fetch_sales_new]
```

`Airflow`에서의 `Graph`에는 아래와 같이 나온다. 
![](https://velog.velcdn.com/images/khhh9401/post/bccd7ff7-1db5-4fb6-bbd5-b188cdf0510f/image.png)


### Trigger Rule

만약, 저 `[fetch_sales_new, fetch_sales_old]` 다음에 새로운 테스크를 추가하면 실행될까? Airflow는 다운스트림 테스크의 조건에 대해서 실행할지 스킵할지 실패로 정할지를 정하는데 그것을 `Trigger Rule` 이라고 부른다. 

먼저 위쪽에 `join_dataset` 이라는 테스크를 추가해보자. 

```
...

join_dataset = PythonOperator(
    task_id="join_dataset",
    python_callable=lambda: print("Join Dataset!!")
)

...

pick_erp_system >> [fetch_sales_old, fetch_sales_new] >> join_dataset

```

이렇게 하고 실행하게되면, `join_dataset` 이라는 테스크는 `skipped` 처리된다. 아래와 같은 화면으로 에어플로우 그래프상에서는 나타난다. 

![](https://velog.velcdn.com/images/khhh9401/post/a6525fdd-52dd-42fd-a37d-4703b3b94ac2/image.png)

왜냐하면 기본적인 `Trigger Rule` 은 `All success` 이기 때문이다. 

이때 `join_dataset` 이라는 테스크를 정상적으로 실행시키기 위해서는 `Trigger Rule` 을 `none_failed` 로 변경하면 된다. 

```
join_dataset = PythonOperator(
    task_id="join_dataset",
    trigger_rule="none_failed",
    python_callable=lambda: print("Join Dataset!!")
)
```

### 조건부 테스크 

위에서 설명했던, 코드상에서 조건을 걸어 `task` 를 분기처리하는것은 `Airflow` 의 `Graph` 상에서 파악하기 어렵다고 설명했었는데, 비슷한 예시로 특정 조건에서만 파이프라인을 동작시키고 특정 조건이 만족하지 않을때에는 해당 파이프라인을 `skipped` 처리하는 예시를 설명해보자. 

그럼 이러한 상황이 있다고 가정해보자. 

`pick_erp_system -> fetch_sales_old/new -> join_dataset -> train_model -> deploy_model`

근데 모델을 훈련 후 배포하는 과정이 가장 최신의 모델만 배포하고 싶다면 어떻게 파이프라인을 작성해야할까? 그것을 `Airflow` 에서는 어떻게 구현하는 것이 좋은지 알아보자. 

그럼 위의 파이프라인을 직접 코드로 구현해보자. 

```
with DAG(
    "branch_operator_example",
    schedule_interval="@daily",
    catchup=True,
    start_date=pendulum.datetime(2025, 3, 20, tz="utc")
) as dag:
    
    pick_erp_system=BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=_pick_erp_system
    )

    pick_erp_system >> [fetch_sales_old, fetch_sales_new] >> join_dataset \
        >> train_model >> deploy_model

    latest_only >> deploy_model
```

이런식으로 구현하면 되고, 

`latest_only` 테스크는 아래와같이 구현하면 된다. 

```
def _latest_only(**context):
    right_window = context["data_interval_end"].date()

    now = pendulum.today("UTC").date()
    if now != right_window:
        raise AirflowSkipException("Not the most recent run!")
    print("It is the last only operator")
```

그럼 이런식으로 `Airflow` 상에서 `Graph` 가 그려진다. 

![](https://velog.velcdn.com/images/khhh9401/post/2572a8c5-4d86-4508-b1f5-faf5b6d37e26/image.png)

### Task 간 데이터 공유

`Airflow` 의 여러 블로그 글이나 도큐먼트들을 보면 `Task` 간의 데이터 공유는 `Xcom` 을 통해서 할 수 있다. 그러한 예시들은 많이 있으니, 본 글에서는 `XCom` 사용시 주의사항에 대해 몇가지 알아보자. 

#### XCom 사용시 주의사항

1. `XCom` 이 원자성을 무너뜨리는 결과를 초래할 수 있다. 

API access token을 XCom을 통해서 주고 받는다면? 첫번째 테스크에서 준 access token이 두번째 테스크에서는 만료되어 제대로 동작을 안할 수 있습니다. 이때, 두번째 테스크에서는 access token을 사용하기 전, refresh하는 작업이 필요할 수 있습니다. 

2. `XCom` 데이터는 모두 직렬화가 가능해야 한다. 

3. `XCom` 데이터는 메타스토어에 저장되며, 메타스토어의 크기는 크지 않습니다. 

대용량 데이터를 사용할때에는 `AWS S3` 처럼 대용량 클라우드 스토리지를 위한 커스텀 백엔드가 구성되어있어서 편리하게 `serialize/deserialize` 를 할 수 있으니 이런 방법을 적극 고려해야 한다. 