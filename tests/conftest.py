import datetime
import pytest
from airflow.models import DAG

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2025, 4, 5),
            "end_date": datetime.datetime(2025, 4, 6)
        },
        schedule=datetime.timedelta(days=1)
    )