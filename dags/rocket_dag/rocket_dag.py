import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from rocket_dag.rocket_dag_utils import fetch_pictures, fetch_launches_json


dag = DAG(
    dag_id="download_rocket_launcher",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

download_launcher = PythonOperator(
    task_id="download_launches",
    python_callable=fetch_launches_json,
    dag=dag
)

get_pictures = PythonOperator(
    task_id="fetch_pictures",
    python_callable=fetch_pictures,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)


download_launcher >> get_pictures >> notify
