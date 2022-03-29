import sqlite3

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from homework2_dag_task4.utils import extract_currency, extract_and_fill_data

CONN = sqlite3.connect("test_db")

dag = DAG(
    dag_id="xcom_dag",
    schedule_interval="@daily",
    start_date=datetime(year=2021, day=1, month=1),
    end_date=datetime(year=2021, day=4, month=1)
    )

extract_currency_op = PythonOperator(
    task_id="extract_currency",
    python_callable=extract_currency,
    op_kwargs={"date": "{{ds}}"},
    dag=dag
)

extract_and_fill_data_op = PythonOperator(
    task_id="extract_and_fill_data",
    python_callable=extract_and_fill_data,
    op_kwargs={"date": "{{ds}}", "table_name": "data", "conn": CONN},
    dag=dag
)


extract_currency_op >> extract_and_fill_data_op

dag.doc_md = "Dag for task 4 with x_com process"
extract_currency_op.doc_md = "Extract current currency from https and send it to xcom"
extract_and_fill_data_op.doc_md = "Extract data https and add column with currency rate and load it to db"

