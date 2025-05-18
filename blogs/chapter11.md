
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

----- 

Github: https://github.com/hhk22/apache-airflow/tree/chapter11

-----

# Chapter 11. Airflow 모범 사례

-----


# 깔끔한 DAG 작성

## 정해진 코드 스타일을 따르자. 

이건 `Airflow` 에서만 국한된것이 아닌, `Python` 코드를 작성할때의 스타일을 통일하기 위해, `linter(pylint, flake8)`, `formatter(ruff, black)` 과 같은 것들을 사용하여 코드 스타일을 통일하게 하자. 


물론 `Airflow` 와 관련된 코드를 작성하는데에도, 정해진 스타일을 준수하는 것이 좋다. 

```
# style I
with DAG(...) as dag:
    task1 = PythonOperator(...)
    task2 = PythonOperator(...)

# style II
dag = DAG(...)
task1 = PythonOperator(..., dag=dag)
task2 = PythonOperator(..., dag=dag)
```

```
# style I
task1 >> task2

# style II
task1.set_downstream(task2)
```

## 중앙에서 자격증명관리

코드를 작성하다보면, 개발자가 로컬에 자격증명관리를 저장하는 경우가 있을 수 있다. 
`Airflow` 에서는 자격증명과 관련된 정보들을 저장소에 저장해두고 관리하면 좋다. (한곳에서 관리할 수 있으니까)

```
from airflow.hooks.bask_hook import BaseHook

def _fetch_data(conn_id, **context):
    credentials = BaskHook.get_connection(conn_id)

fetch_data = PythonOperator(
    task_id="fetch_data",
    op_kwargs={"conn_id": "my_conn_id"},
    dag=dag
)
```

## 변수 정보를 일관성 있게 저장 / DAG 구성 시 연산 부분 배제

변수정보를 일관성있게 할려면 어떻게 해야할까?

`Airflow` 에서는 중앙에서 변수들을 저장할 수 있는 `Variable` 이라는 것이 있다. 

```
from airflow.models import Variable

input_path = Variable.get("dag_input_path")
output_path = Variable.get("dag_output_path")

fetch_data = PythonOperator(...)
```

이런식으로 작성할 수 있는데, 전역범위는 `Variable.get` 이 있으므로, 
해당 영역은 웹서버나 웹스케줄러에 의해 주기적으로 호출이 되므로 다소 비효율적으로 
동작할 수 있음을 유의해야 한다. 

이렇게되면, 웹서버나 웹스케줄러는 전역범위에 있는 코드가 정상적으로 실행될 수 있는
권한같은것도 신경 써줘야 한다. 

이러한 코드는 비효율적으로 동작하기때문에, 가급적이면 변수정보를 저장하는 코드들은
가급적이면 함수내에 선언(스캔시 로딩이 되지 않고, 워커가 실행시 로드)하는 것이 좋다. 아래처럼

```
from airflow.models import Variable
from airflow.operators.python import PythonOperator

def fetch_data_fn():
    # Variable.get() 호출을 함수 내부로 이동
    input_path = Variable.get("dag_input_path")
    output_path = Variable.get("dag_output_path")
    
    ...

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_fn,
    ...
)
```

전역범위에 있는 코드들은 스케줄러에의해 실행되기 때문에 연산량이 많은 함수같은 것들은
배제 해야 한다.

```
task1 = PythonOperator(
    ...
)

# very heavy job
my_value = do_some_long_computation()

task2 = PythonOperator(
    ...
)

```

여기서, `my_value = do_some_long_computation()` 부분이 매번 실행되기때문에
매 스캔마다 실행되기때문에, `Airflow` 의 동작이 매우 비효율적으로 동작할 수 있다. 

특히 이런식의 DAG가 로드가 된다면, `my_value = do_some_long_computation()` 이 
코드가 `task` 프로세스가 긴시간동안 중단될 수 있다. 


## 재현가능한 테스크 설계

`Airflow` 에서의 테스크는 언제 어디서 실행되더라도 **항상 동일한 결과** 를 도출해야 합니다. 

이는, `Airflow` 에서 `DAG` 내의 일부 테스크를 수정한 이후에 다시 실행하는 경우가 굉장히
빈번하게 일어나고, 이럴때 항상 동일한 결과를 도출할 수 있도록 설계해야한다. 

그렇지 않다면 굉장히 복잡한 상황이 발생할 수 있다. 

일반적으로 테스크를 다시 실행할때, 같은 데이터의 중복 생성이나 의도치 않은 결과를 도출을 피할 수 있도록
여러가지 방법을 고안하는 것이 필요하다. 

앞서 출련된 데이터를 덮어 쓰도록 설정하여서 멱등성을 강제하거나 데이터 중복체크를 통해 결과를 쓰지 않는 등
멱등성을 고안하기 위해 여러가지 상황에 맞게 테스크를 설계해야한다. 

## 효율적인 데이터 처리

### 데이터 처리량 제한

필요한 데이터만을 가지고서 파이프라인을 작성. 

예를들어, 특정 고객에 대한 제품의 월별 판매량을 계산하는 데이터 프로세스가 있다고 가정하면, 

1. 전체 매출 데이터와 전체 고객 데이터 조인 -> 특정 고객에 대한 필터링 및 집계

2. 전체 매출 데이터와 고객 데이터에서 특정 고객에 대한 데이터 필터링 -> 집계 및 조인

데이터 처리프로세스에서 선별적으로 제한하는 파이프라인인 2번의 과정에서 처리량을 훨씬 줄일 수 있다. 

### 중간 단계 데이터 캐싱

API 요청을 통한 데이터들은 보통 스냅샷/버전 기능을 제공하지 않는 경우가 많다. 

따라서, 중간중간의 데이터들을 가장 원시버전을 사용가능하도록 보관하는것이 좋다. 

### 로컬 파일시스템에 데이터 저장 방지

`Airflow` 에서는 다중 워커시스템으로 돌아가는 경우가 많으므로, 로컬 파일시스템에서 접근하도록
설계하는 것을 적합하지 않다. 여러개의 워커들이 접근이 가능한 공유 스토리지를 사용하자. 

### 외부/소스 시스템으로 작업 이전

`Airflow` 는 오케스트레이션 도구로써 사용할때 가장 적합하다. 직접 코드를 작성해 실행시키는 것보다, 

실제 작업에 대한 부분을 다른 시스템으로 이관하는 것이 좋다. 가령, 데이터베이스의 쿼리문을 `PythonOperator` 에서

직접 요청해서 처리하는것이 아닌, 필요한 쿼리를 데이터베이스 시스템에서 직접 수행하는 방식으로 설계하자. 









