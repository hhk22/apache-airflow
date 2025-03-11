
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

제가 자주출근하는 2호선 출근 데이터를 가지고서 코드를 작성했습니다. 

-----

- Chapter 04. Airflow 콘텍스트

----

## Airflow Context

Airflow 에서는 Task간의 내용을 공유할 수 있도록 <b>Context</b> 라는 것을 제공한다. 그럼 실제 Airflow 에서 <b>Context</b> 를 출력해보고 어떠한 것들이 있는지 확인해보자. 

```
dag = DAG(
    "check_context",
    default_args=default_args,
    schedule_interval=None
)

def _print_context(**context):
    print(context.keys())


python_task = PythonOperator(
    task_id="python_task",
    python_callable=_print_context,
    dag=dag
)
```

<b>Context</b> 의 키값들만 출력해보면 이러한 값들을 확인할 수 있다. 

```
['conf', 'dag', 'dag_run', 'data_interval_end', 'data_interval_start', 'outlet_events', 'ds', 'ds_nodash', 'execution_date', 'expanded_ti_count', 'inlets', 'inlet_events', 'logical_date', 'macros', 'map_index_template', 'next_ds', 'next_ds_nodash', 'next_execution_date', 'outlets', 'params', 'prev_data_interval_start_success', 'prev_data_interval_end_success', 'prev_ds', 'prev_ds_nodash', 'prev_execution_date', 'prev_execution_date_success', 'prev_start_date_success', 'prev_end_date_success', 'run_id', 'task', 'task_instance', 'task_instance_key_str', 'test_mode', 'ti', 'tomorrow_ds', 'tomorrow_ds_nodash', 'triggering_dataset_events', 'ts', 'ts_nodash', 'ts_nodash_with_tz', 'var', 'conn', 'yesterday_ds', 'yesterday_ds_nodash', 'templates_dict']
```

이러한 <b>Context</b> 는 `key/value` 형태로 전달되기 때문에 아래와 같이 원하는 변수는 따로 떼어내어 작성할 수도 있습니다. 

```
def _print_context(excution_date, **context):
    print(execution_date)
    print(context.keys())  # execution_date 값이 없음.
```

## Task간 데이터 전달

`Airflow Task` 는 독립적인 테스크로 간주되고, 그렇기 때문에 같은 컴퓨터에서 동작한다고 생각하면 안된다. 

`Airflow Task` 간의 데이터 전달을 하기 위해서 `Xcom` 을 제공한다. 위에서 볼 수 있듯이 `ti` 라는 키값에 다른 테스크의 데이터를 받아올 수 있다. 

문제는 이 데이터들이 `Airflow 메타스토어` 에 저장이되는데, 용량에 한계가 있는 경우가 많고, 대용량을 처리하기보다는 저용량의 데이터를 빠르게 가져올 수 있도록 설계가 되어있어서 대량의 데이터를 주고받게 되면, `Airflow` 전체 성능이 안좋아지는 결과를 가져온다. 

따라서, 대용량의 데이터는 보통 `디스크에 직접 쓰는 경우` 가 많으며, 

보통 대용량의 데이터를 sql문으로 작성한 후, `postgresql operator` 와 같은 `task` 를 만들어 sql문을 실행해 데이터베이스에 넣는작업을 실행하는 경우가 많다. 


References

- https://www.yes24.com/Product/Goods/107878326

- https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html 

- https://data.seoul.go.kr/

----
