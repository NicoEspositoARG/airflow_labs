from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="01_dependencias",
    start_date=datetime(2021, 9, 7),
    catchup=False,
    schedule_interval="@daily",
    tags=["ejercicios"],
) as dag:

    t1 = DummyOperator(task_id="task_1")
    t2 = DummyOperator(task_id="task_2")
    t3 = DummyOperator(task_id="task_3")
    t4 = DummyOperator(task_id="task_4")

    t1 >> [t2, t3] >> t4
