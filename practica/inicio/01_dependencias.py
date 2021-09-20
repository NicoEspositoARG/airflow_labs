from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

# with DAG(
#     dag_id="01_dependencias",
#     start_date=datetime(2021, 9, 7),
#     catchup=False,
#     schedule_interval="@daily",
#     tags=["ejercicios"],

# ) as dag:
