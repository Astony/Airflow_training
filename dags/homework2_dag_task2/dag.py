from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def my_func(hello, date, **context):
    print(hello)
    print(date)
    print(context['task'])


with DAG(
    "dag",
    schedule_interval="@daily",
    start_date=datetime(day=1, month=1, year=2021),
    end_date=datetime(day=10, month=1, year=2023),
) as dag:

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=my_func,
        op_kwargs={"hello": "Hello World!", "date": "{{execution_date}}"},
    )
