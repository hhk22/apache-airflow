
------

해당글은 [Apache Airflow 기반의 데이터 파이프라인](https://www.yes24.com/Product/Goods/107878326)을 읽고 정리한 내용입니다. 

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

