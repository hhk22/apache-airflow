
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

----- 

Github: https://github.com/hhk22/apache-airflow/tree/chapter09

-----

- Chapter 09. Airflow Test

----

 # 모든 DAG에 대한 무결성 테스트

 이 테스트는 단순히 DAG가 정상적으로 돌아가는지에 대한 테스트이다. 가령, 아래와 같은 코드가 있다면, 

 ```
 t1 = DummyOperator(...)
 t2 = DummyOperator(...)
 t3 = DummyOperator(...)

 t1 >> t2 >> t3 >> t1
 ```

이건 비즈니스로직이 어떻게 되던간에 오류를 일으키는 코드이다. 순환 노드로 구성되어있기 때문이다. 이러한 기본적인 DAG의 규칙들을 어기는것을 무결성테스트로 잡아낼 수 있다. 

그럼 실제로 무결성테스트 코드를 작성해보자.  
대략적인, 설명은 `DAG.py` 파일을 `importlib` 라이브러리를 통해 가져와서, `DAG` 객체를 가져온다음, `airflow util` 을 통해서 `cycle` 의 여부를 파악하는 코드이다. 

```
@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)

    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    dag_objects: list[DAG] = [
        var for var in vars(module).values() if isinstance(var, DAG)
    ]
    
    for dag in dag_objects:
        check_cycle(dag)
```

이런식으로 비즈니스 로직이 아닌 `DAG` 자체의 문제점을 테스트할 수 있다. 

# CI/CD 파이프라인

여러가지 `CI/CD` 파이프라인이 있겠지만, 책에서는 많이 쓰이는 `Github Actions` 를 소개했다. 그리고 `pylint`, `flake8`, `black` 들로 구성했다. 

사실 이러한 정적테스트 도구들은 중요한 대상이 아니라 넘어간다.  

중요한 부분은 `pytest` 를 `Github Actions` 에서 돌리고, 단위테스트를 작성하는 부분을 집중적으로 살펴보자. 

## 단위 테스트 작성

가장 간단한 테스트코드를 작성해보자. 

```
def test_example():
    task = BashOperator(
        task_id="test",
        bash_command="echo hello"
    )

    result = task.execute(context={})
    assert result == "hello"

$ pytest tests/test_bash_operator
>>
tests/test_bash_operator.py .  
=========== 1 passed in 3.81s =====================
```

이번엔 좀 더 복잡한 방식으로 `DAG` 를 작성해 테스트 할 수 있다.  
먼저, `Custom Operator` 를 아래와 같이 작성되어 있다고 하자. 

```
class CustomRedisHook(BaseHook):
    def __init__(self, redis_conn_id='my_redis'):
        super().__init__()
        self.redis_conn_id = redis_conn_id
        self._client = None

    def get_conn(self):
        if not self._client:
            conn = self.get_connection(self.redis_conn_id)
            self._client = redis.Redis(
                host=conn.host,
                port=conn.port,
                password=conn.password,
                db=0,
                decode_responses=True
            )
        return self._client

    def ping(self):
        client = self.get_conn()
        return client.ping()

    def __enter__(self):
        self.get_conn()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None


class CustomPythonOperator(BaseOperator):
    def execute(self, context):
        with CustomRedisHook() as hook:
            if hook.ping():
                print("PONG from redis!")
```

`Redis` 를 연결하는 `Custom Hook` 을 구현했고, 그것을 테스트하는 코드를 작성해보자. 

```
def test_redis_ping_method_mocked(mocker):
    mock_ping = mocker.patch.object(CustomRedisHook, "ping", return_value=True)
    mock_connection = mocker.patch.object(
        CustomRedisHook, 
        "get_conn", 
        return_value=redis.Redis(host="redis")
    )

    task = CustomPythonOperator(task_id="test_task")
    task.execute(context=None)

    mock_ping.assert_called_once()
    mock_connection.assert_called_once()
```

> 이코드를 실행하기 위해선 `pytest-mock` 이 설치되어있어야 한다. 

`ping`, `get_conn` 메소드를 `mocking` 하고, 커스텀 오퍼레이터를 실행시켜, 각각의 메소드들이 실행됐는지 체크하는 함수이다. 


## 테스트에서 Task Context로 작업하기 

`airflow` 에서는 `execute` 로 실행할때에는 비즈니스 로직을 검사할때에만 가능하다.  
실질적으로, `airflow` 메타스토어를 사용하거나, `XCom` 과 같은 데이터를 사용하지 않는다.  

반면, 실제 Airflow처럼 전체 Task 생명주기를 테스트하려면 **`task.run()`** 을 사용해야 한다.

코드를 직접 살펴보게되면, 아래와 같다. 

```
from airflow.models.baseoperator import BaseOperator
import datetime

import pendulum
import pytest

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

DATA_INTERVAL_START = pendulum.now("UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "my_custom_operator_dag"
TEST_TASK_ID = "my_custom_operator_task"


class SampleOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date")

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._end_date = end_date
    
    def execute(self, context):
        print(context)
        context["ti"].xcom_push(key="start_date", value=self._start_date)
        context["ti"].xcom_push(key="end_date", value=self._end_date)
        return "context"


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        SampleOperator(
            start_date="{{ prev_ds }}",
            end_date="{{ next_ds  }}",
            task_id=TEST_TASK_ID
        )
    return dag


def test_my_custom_operator_execute_no_trigger(dag: DAG):
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(
        task_id=TEST_TASK_ID
    )

    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.render_templates()
    ti.run(ignore_ti_state=True)
    
    start_date = ti.xcom_pull(task_ids=TEST_TASK_ID, key="start_date")
    end_date = ti.xcom_pull(task_ids=TEST_TASK_ID, key="end_date")

    assert ti.state == TaskInstanceState.SUCCESS
    assert start_date == DATA_INTERVAL_START.strftime("%Y-%m-%d")
    assert end_date == DATA_INTERVAL_END.strftime("%Y-%m-%d")

```

이런식으로 구성하고 테스트 코드를 돌리면 된다.  
여기서 `{{prev_ds}}` 와 같은 `jinja` 변수를 선언하는데, `task.execute` 를 실행하게되면 `rendering_template` 이 실행되지 않아 `start_date` 에 올바른 변수가 들어가지않는다.  

하지만, `run` 함수를 실행하게되면, 자동적으로 `prev_ds`, `ds` 값을 렌더링해서 변수로 넣어주게 된다.  

코드도 돌려보면 알겠지만, 실질적인 `airflow metastore` 를 사용하게 된다. 

만약 실질적인 환경에서 테스트코드를 돌려야 할 상황이라면 `task.run` 을 사용하자. 






