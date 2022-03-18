import sqlite3
import pandas as pd
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

CONN = sqlite3.connect("train_db")


def extract_data(url, tmp_file, **context):
    pd.read_csv(url).to_csv(tmp_file)


def transform_data(group, agreg, tmp_file, tmp_agg_file, **context):
    data = pd.read_csv(tmp_file)
    data.groupby(group).agg(agreg).reset_index().to_csv(tmp_agg_file)


def load_data(tmp_file, table_name, conn=CONN, **context):
    data = pd.read_csv(tmp_file)
    data["insert_timme"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists="replace", index=False)


dag = DAG(
    dag_id="train_dag",
    default_args={'owner':'airflow'},
    schedule_interval="@daily",
    start_date=days_ago(1)
)

extract_data_op = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    op_kwargs={
        'url': 'https://raw..githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv',
        'tmp_file':'/tmp/file.csv'
    },
    dag=dag
)


transform_data_op = PythonOperator(
    task_id="transform_data",
    python_callable=extract_data,
    op_kwargs={
        'tmp_file':'tmp/file.csv',
        'tmp_agg_file':'/tmp/file_agg.csv',
        'group':['A', 'B', 'C'],
        'agreg': {"D": sum}
    },
    dag=dag
)


load_data_op = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    op_kwargs={
        'tmp_file':'tmp/file.csv',
        'table_name': "test_table"
    },
    dag=dag
)


email_op = EmailOperator(
    task_id="email",
    to='savrushkin.aa@ggmail.com',
    subject='test_email',
    html_content=None,
    files=['/tmp/file.csv'],
    dag=dag
)

extract_data_op >> transform_data_op >> [load_data_op, email_op]