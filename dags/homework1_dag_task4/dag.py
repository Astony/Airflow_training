import sqlite3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from homework1_dag_task4.utils import extract_currency, extract_data, join_tables

CONN = sqlite3.connect("test_db")

dag = DAG(dag_id="dag4", schedule_interval="@daily", start_date=days_ago(1))

extract_currency_op = PythonOperator(
    task_id="extract_currency",
    python_callable=extract_currency,
    op_kwargs={"date": "2021-01-01", "table_name": "currency", "conn": CONN},
    dag=dag
)


extract_data_op = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    op_kwargs={"date": "2021-01-01", "table_name": "data", "conn": CONN},
    dag=dag
)


join_tables_op = PythonOperator(
    task_id="join_tables",
    python_callable=join_tables,
    op_kwargs={
        "join_table": "join_table",
        "table1": "currency",
        "table2": "data",
        "conn": CONN,
    },
    dag=dag
)

extract_currency_op >> extract_data_op >> join_tables_op

dag.doc_md = "Dag for task 4 with simple ETL process"
extract_currency_op.doc_md = "Extract current currency from https and load it to db"
extract_data_op.doc_md = "Extract data https and load it to db"
join_tables_op.doc_md = "Join previous tables into one and save result to db"
