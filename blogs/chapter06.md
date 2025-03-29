
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

-----

- Chapter 06. Airflow Trigger

----


# 센서를 이용한 풀링조건

우리는 지금까지 일정한 주기로 `Task`를 실행하는 스케줄링 방식을 사용해왔다. 또는 우리는 지금까지 주기적 실행 방식으로 `Task`를 수행해왔다.

그런데 모든 작업들이 이러한 일정한 주기로 시작되면 비효율적으로 돌아가는 경우가 많다. 

예를들어, 

```
copy_to_raw_supermarket_1 --> process_supermarket_1 -->

copy_to_raw_supermarket_2 --> process_supermarket_2 -->
                                                            create_metrics
copy_to_raw_supermarket_3 --> process_supermarket_3 -->

copy_to_raw_supermarket_4 --> process_supermarket_4 -->
```

이러한 워크플로우가 있다고 하자. `supermarket_1` 데이터는 오후2시에 도착하고, `supermarket_2` 데이터는 오후4시에 도착하고, `supermarket_3` 데이터는 오후10시에 도착하고, `supermarket_4` 데이터는 오후11시에 도착한다라고 가정한다면, 스케줄링 워크플로우는 11시 이후에 시작해야한다. 

게다가, `supermarket_1` 워크플로우는 11시에 해당 워크플로우가 시작한다고 하면 데이터가 도착하고나서 9시간후에 시작하게 된다. 

만약, `supermarket_1,2,3,4` 가 각각 `data` 가 특정위치에 도착한 즉시 시작하게 할 수 있다면 기존의 스케줄링 워크플로우보다 빠르게 즉시 시작할 수 있다. 

그래서 이럴경우, `Sensor Operator` 가 필요하다. 

## Sensor Operator

`Sensor` 는 특정 조건을 계속 탐색해서 조건이 `True` 이면, 성공하고 `False` 이면 타임아웃조건이 될때까지 계속 확인한다. 

```
from airflow.sensors.filesystem import FileSensor

wait_for_supermarket_1=FileSensor(
    task_id="wait_for_supermarket_1",
    filepath="/data/supermarket1/data.csv"
)
```

여기서 보게되면, 해당 `filepath` 위치에 파일이 있을때까지 계속 탐색한다.  
탐색하는 행위를 `Airflow` 에서는 `Poking` 이라고 하고, 해당 조건을 조절할 수 있다. 

그래서 이런식으로, 파이프라인을 변경할 수 있다. 

`task_id` 는 줄바뀜때문에 간단하게 바꿧다. 

```
wait_for_market_1 -> copy_to_raw_market_1 -> process_market_1 ->

wait_for_market_2 -> copy_to_raw_market_2 -> process_market_2 ->
                                                                 create_metrics
wait_for_market_3 -> copy_to_raw_market_3 -> process_market_3 ->

wait_for_market_4 -> copy_to_raw_market_4 -> process_market_4 ->
```

이렇게 바뀌게 되면, 아까 예시를 들었던것을 다시 설명해보면, 각각의 `market` 워크플로우들은 `wait_for_market_*` 이 발동되는 시점에 발동될 수 있다. 


### 원할하지 않는 흐름의 센서 처리 

만약 센서가 정상적으로 동작하지 않는다면 어떻게 될까?

`market_1,2,3,4` 의 워크플로우에서 `market_1` 의 `sensor` 만 성공하고, `market_2,3,4` 의 `sensor` 들은 실패한다면 어떻게 되는지 알아보자. 

센서의 기본 타임아웃은 7일이고, 이걸 감안하고 가정해보면

Day1 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
Day2 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
Day3 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
Day4 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
Day5 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
Day6 : 슈퍼마켓1 이 성공, 슈퍼마켓 2,3,4가 3개의 테스크를 차지하며, 폴링중...
...
이후 최대 테스크 수에 도달하면 새로운 테스크가 생성되지 못하고 중지된다. 

기존의 실패했던 `Sensor` 테스크들이 계속 슬롯을 차지합니다. 그렇게 실패한 `Sensor` 들이 계속 슬롯을 차지하며 테스크들이 넘치는 현상을 <b>Sensor Deadlokc</b> 이라고 한다. 

`Airflow` 에서 `Sensor` 클래스는 `mode` 인수에 대해 `poke` 또는 `reschedule` 를 제공한다. 

`poke` 는 테스크가 실행되는 동안에는 슬롯을 차지하지만, `reschedule` 은 `Sensor` 테스크가 포킹동작을 할때에만 슬롯을 차지합니다. 

그래서, `FileSensor` 에 대해서, 아래와 같은 방식으로 작성하게되면 `Sensor DeadLock` 현상이 재현되지 않는다. 

```
wait_for_supermarket_1=FileSensor(
    task_id="wait_for_supermarket_1",
    filepath="/data/supermarket1/data.csv",
    mode="reschedule"
)
```

## 다른 DAG를 트리거하기

그러면, 위의 각각의 `market_1,2,3,4` 들이 실행되고 나서, `create_metrics` 를 어떻게 실행해야할까? 기존의 방식으로 작성해야한다면, 각각의 `workflow` 들이 끝나고 `create_metrics` 들을 실행하는 방식을 작성할 수 있다. 

```
wait_for_market_1 -> copy_to_raw_market_1 -> process_market_1 -> create_metrics_1

wait_for_market_2 -> copy_to_raw_market_2 -> process_market_2 -> create_metrics_2
                                                                 
wait_for_market_3 -> copy_to_raw_market_3 -> process_market_3 -> create_metrics_3

wait_for_market_4 -> copy_to_raw_market_4 -> process_market_4 -> create_metrics_4
```

그런데 이렇게 되면, `create_metrics_1,2,3,4` 들이 비슷한 일을 하지만, 중복이 된다는 단점이 있다. 

그러한 대안으로 하나의 `DAG` 에서 다른 `DAG` 를 호출하는 `TriggerDagRunOperator` 를 사용할 수 있다. 

### TriggerDagRunOperator

`dag1` 에서 속한 하나의 `task` 가 마지막에 다른 `dag` 를 호출하는 `TriggerDagRunOperator` 는 다음과 같이 작성할 수 있다. 

```
dag1 = DAG(
    dag_id="ingest_supermarket_data",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *"
)

for supermarket_id in range(1, 5):
    # ...
    trigger_create_metrics_dag=TriggerDagRunOperator(
        task_id=f"trigger_create_metrics_dag_supermarket_{supermarket_id}",
        trigger_dag_id="create_metrics",
        dag=dag1
    )

dag2 = DAG(
    dag_id="create_metrics",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None
)
```

이런식으로 작성할 수 있고, `create_metrics` 의 `DAG` 에는 `compute_difference`, `update_dashboard` 가 존재한다면 이런식으로 작성될 수 있다. 

![](https://velog.velcdn.com/images/khhh9401/post/59e4454a-6211-47a4-a2c3-962fbb5d2488/image.png)

이런식으로 중복된 `dag`, `task` 작성을 피할 수 있다. 

----
#### TriggerDagRunOperator 의 Backfill

1. 테스크 삭제는 동일한 `DAG` 안의 테스크만 지워집니다. 또 다른 `DAG` 안에서 `TriggerDagRunOpeartor` 의 다운스트림 테스크는 지워지지 않습니다.  

만약 이러한 구조가 있다고 생각해보자. 

```
dag1:
    task1 --> trigger_dag2
    task2

    task1 >> task2

dag2:
    task3 >> task4
```

이러한 구조가 있다고 생각하면, 여기서 `task2` 를 취소하면 `task2` 만 재실행되고 `task1`, `dag2` 에는 영향을 끼치지 않는다. 

2. `TriggerDagRunOperator` 를 포함한 `DAG` 에서 테스크를 삭제하면 이전에 트리거된 해당 `DAG` 실행을 지우는 대신에 새로운 `DAG` 실행이 트리거 된다. 

```
dag1:
    task1 --> trigger_dag2
    task2

    task1 >> task2

dag2:
    task3 >> task4
```

여기서 `task1` 을 삭제하게되면, `task1` 의 실행이력은 남아있지만, `dag2` 의 `task3, 4` 는 새로 실행된다. 

----

### ExternalTaskSensor

만약 이러한 상황이라면 어떻게 해야할까?  

`DAG4` 가 `DAG1,2,3` 가 완료되고 나서 실행되어야 한다면?

이러한 상황에서는 `ExternalTaskSensor` 를 사용하자. 

```
etl(dag1,2,3) <-- | DAG4 | wait_for_etl_dag1 -->

etl(dag1,2,3) <-- | DAG4 | wait_for_etl_dag2 -->  report

etl(dag1,2,3) <-- | DAG4 | wait_for_etl_dag3 -->

```

코드는 간단하게 이런식으로 작성하면 된다. 

```
with DAG("dag4", ...):
    wait = ExternalTaskSensor(
        task_id="wait_for_process_supermarket",
        external_dag_id="dag3",
        external_task_id="process_supermarket"
    )

    ...
```

근데 여기서 중요한건, `ExternalTaskSensor` 가 `dag3` 에 `process_supermarket` 의 `task` 의 상태를 확인할때, 자기가 트리거된 시간에서와 동일한 `task` 의 상태를 확인한다는 것이다. 

예를들어, `ExternalTaskSensor` 가 `2025.3.29 15:00` 에 트리거 됐다면, `2025.3.29 15:00` 에 트리거된 `process_supermarket` 의 상태가 `success` 인지를 확인한다는 것이다. 만약 `ExternalTaskSensor` 가 확인해야할 `task` 의 시간대가 다르다면 `execution_delta` 를 이요하면 된다. 

```
with DAG("dag4", ...):
    wait = ExternalTaskSensor(
        task_id="wait_for_process_supermarket",
        external_dag_id="dag3",
        external_task_id="process_supermarket",
        excution_delta=datetime.timedelta(hours=4)
    )
    ...
```

여기서, `ExternalTaskSensor` 는 `2025.3.29 15:00` 에 트리거 됐다면, `2025.3.29 11:00` 에 트리거된 `process_supermarket` 의 상태를 확인할 것이다. 

이러한 식으로 두 테스크간의 시간계산을 잘 하는 것이 중요하다. 