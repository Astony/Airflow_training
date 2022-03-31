import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
import sqlite3
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

CONN = sqlite3.connect("example_db")

dag = DAG(
    dag_id='sqlite_dag',
    schedule_interval='@daily',
    start_date=datetime.today(),
)

# Задача для создания таблицы в sqlite базе данных
create_table_data = SqliteOperator(
    task_id='create_table_data',
    sql="""
    CREATE TABLE if not exists data (
        currency TEXT,
        value INT,
        date DATE
    );
    """,
    dag=dag,
)
# Задача для создания таблицы в sqlite базе данных
create_table_currency = SqliteOperator(
    task_id='create_table_currency',
    sql="""
    CREATE TABLE if not exists currency (
        date DATE,
        code TEXT,
        rate TEXT,
        base TEXT,
        start_date DATE,
        end_date DATE
    );
    """,
    dag=dag,
)


def insert_sqlite_hook(url, table_name):
    sqlite_hook = SqliteHook()
    # Скачиваем данные
    data = pd.read_csv(url)
    # Вставляем данные
    sqlite_hook.insert_rows(table=table_name, rows=data.to_records(index=False), target_fields=list(data.columns))

# Задача для добавления данных из pandas DataFrame
insert_sqlite_data = PythonOperator(
    task_id='insert_sqlite_data',
    python_callable=insert_sqlite_hook,
    op_kwargs={'url': 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/2021-01-01.csv', 'table_name': 'data'},
    dag=dag,
)
# Задача для добавления данных из pandas DataFrame
insert_sqlite_currency = PythonOperator(
    task_id='insert_sqlite_currency',
    python_callable=insert_sqlite_hook,
    op_kwargs={'url': 'https://api.exchangerate.host/timeseries?start_date=2021-01-01&end_date=2021-01-01&base=EUR&format=csv&symbols=USD', 'table_name': 'currency'},
    dag=dag,
)

# Ваше задание

# Создать таблицу через SQLiteOperator
create_table_join = SqliteOperator(
    task_id='create_table_join',
    sql="""
    CREATE TABLE if not exists join_table (
        currency TEXT,
        value INT,
        date DATE,
        code TEXT,
        rate TEXT,
        base TEXT
    );
    """,
    dag=dag,
)

# Объедините данные через SQLiteOperator
join_data = SqliteOperator(
    task_id='join_data',
    sql=""" INSERT INTO join_table (currency, value, date, code, rate, base)
    SELECT D.currency, D.value, D.date, C.code, C.rate, C.base FROM data AS D JOIN currency AS C ON D.date = C.date""",
    dag=dag,
)

[create_table_data, create_table_currency, create_table_join] >> insert_sqlite_data >> insert_sqlite_currency >> join_data