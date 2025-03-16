
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














- 선형 의존성 유형 <====> 팬인/팬아웃 유형

--> 선형의존성 유형: 간단하고 명확하게 정의될 수 있다. 

팬인 / 팬아웃 -> 분기처리 어떻게?

1. 코드로 분기처리

항상 가능한것은 아니다. 코드로 분기처리하면 어떤 시스템에서 어떤 파이프라인을 타는지 파악하기 힘들다. 

2. Branch Operator로 분기처리하기. 

trigger_rule 을 이용해야함. 

좀더 명확한 branch operator 구조를 가져가기 위해 dummy operator를 추가함. 

3. 조건부 테스크

4. 트리거 규칙

5. Xcom 데이터 사용

- template_dict 사용법?
- Xcom 사용시 주의사항
    1. 사용량 제한
    2. 직렬화가 가능해야한다
    3. 잘못사용하면 원자성이 무너진다. ex) API 토큰




