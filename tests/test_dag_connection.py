from dag_custom_operator import CustomRedisHook, CustomPythonOperator
from airflow.models.connection import Connection
import redis

def test_redis_ping_method_mocked(mocker):
    mock_ping = mocker.patch.object(CustomRedisHook, "ping", return_value=True)
    mock_connection = mocker.patch.object(
        CustomRedisHook, 
        "get_conn", 
        return_value=redis.Redis(host="redis")
    )

    task = CustomPythonOperator(task_id="test_task")
    task.run()
    # task.execute(context=None)


    mock_ping.assert_called_once()
    mock_connection.assert_called_once()

