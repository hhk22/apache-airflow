from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from datetime import datetime
import redis


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


