from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

# from airflow.decorators import dag


args = {
    "owner": "airflow",
}
# context manager

with DAG(
    dag_id="primer_dag",
    start_date=datetime(2021, 9, 7),
    end_date=datetime(2021, 12, 8),
    schedule_interval="@daily",
    tags=["ejemplos"],
) as dag:

    t1 = DummyOperator(task_id="task_1")
    t2 = DummyOperator(task_id="task_2")
    t3 = DummyOperator(task_id="task_3")

    # Downstream
    t1 >> t2 >> t3
    # # or
    # t1.set_downstream(t2)
    # t2.set_downstream(t3)

    # Upstream
    # t3 << t2 << t1
    # # or
    # t3.set_upstream(t2)
    # t2.set_upstream(t1)


# constructor standard

# my_dag = DAG(
#     dag_id="primer_dag",
#     start_date=datetime(2021, 9, 7),
#     end_date=datetime(2021, 12, 8),
#     schedule_interval="@daily",
#     tags=["ejemplos"],
# )

# op = DummyOperator(task_id="task_a", dag=my_dag)

# dag decorator

# @dag(
#     start_date=datetime(2021, 9, 7),
#     end_date=datetime(2021, 12, 8),
#     schedule_interval="@daily",
#     tags=["ejemplos"],
# )
# def primer_dag():
#     DummyOperator(task_id="task_a")


# dag = primer_dag()
