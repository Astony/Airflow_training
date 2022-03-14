from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from time_dag.tasks import get_stats_csv, create_folders

folders = {'input_folder':'/tmp/events',
           'output_folder':'/tmp/stats'}
files_paths = {
        'input_path':'/tmp/events/{{ds}}.json',
        'output_path':'/tmp/stats/{{ds}}.csv'
    }

dag = DAG(
    dag_id="time_dag",
    start_date=datetime(year=2022, day=10, month=3),
    schedule_interval='@daily'
)

create_folders = PythonOperator(
    task_id="create_folders",
    python_callable=create_folders,
    templates_dict=folders,
    dag=dag

)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=
                 "curl -o /data/events/{{ds}}.json "
                 "http://localhost:5000/events?"
                 "start_date={{ds}}?"
                 "&end_date={{next_ds}}",
    dag=dag
)
get_stats = PythonOperator(
    task_id='get_stats_in_csv',
    python_callable=get_stats_csv,
    templates_dict=files_paths,
    dag=dag
)

create_folders >> fetch_events >> get_stats