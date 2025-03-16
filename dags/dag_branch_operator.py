from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random


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