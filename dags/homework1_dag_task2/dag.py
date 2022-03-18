from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

dag = DAG("dag1", schedule_interval=timedelta(days=1), start_date=days_ago(1))
t1 = DummyOperator(task_id="task_1", dag=dag)
t2 = DummyOperator(task_id="task_2", dag=dag)
t3 = DummyOperator(task_id="task_3", dag=dag)
t4 = DummyOperator(task_id="task_4", dag=dag)
t5 = DummyOperator(task_id="task_5", dag=dag)
t6 = DummyOperator(task_id="task_6", dag=dag)
t7 = DummyOperator(task_id="task_7", dag=dag)
#
[t1, t2] >> t5
[t2, t4] >> t6
t3 >> t6
[t4, t5, t6] >> t7

dag = DAG("dag2", schedule_interval=timedelta(days=1), start_date=days_ago(1))
t1 = DummyOperator(task_id="task_1", dag=dag)
t2 = DummyOperator(task_id="task_2", dag=dag)
t3 = DummyOperator(task_id="task_3", dag=dag)
t4 = DummyOperator(task_id="task_4", dag=dag)
t5 = DummyOperator(task_id="task_5", dag=dag)
t6 = DummyOperator(task_id="task_6", dag=dag)
t7 = DummyOperator(task_id="task_7", dag=dag)

[t1, t2] >> t4
[t1, t3, t6] >> t2
[t1, t5] >> t3
t1 >> t5
t1 >> t6
t2 >> t7