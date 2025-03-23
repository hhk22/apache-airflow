from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowSkipException
import random
import pendulum

def _pick_erp_system(**context):
    number = random.choice([1, 2])
    print(f"number: {number}")
    if number == 1:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"
    
def _latest_only(**context):
    right_window = context["data_interval_end"].date()

    now = pendulum.today("UTC").date()
    if now != right_window:
        raise AirflowSkipException("Not the most recent run!")
    print("It is the last only operator")

fetch_sales_old = PythonOperator(
    task_id="fetch_sales_old",
    python_callable=lambda: print("Task A 실행")
)

fetch_sales_new = PythonOperator(
    task_id="fetch_sales_new",
    python_callable=lambda: print("Task B 실행")
)

join_dataset = PythonOperator(
    task_id="join_dataset",
    trigger_rule="none_failed",
    python_callable=lambda: print("Join Dataset!!")
)

latest_only = PythonOperator(
    task_id="latest_only",
    python_callable=_latest_only
)

train_model = PythonOperator(
    task_id="train_model",
    python_callable=lambda: print("train_model!!")
)

deploy_model = PythonOperator(
    task_id="deploy_model",
    python_callable=lambda: print("deploy model!!")
)

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