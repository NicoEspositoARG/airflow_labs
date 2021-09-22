"""DAG ejercitación schedule_interval & DAG Runs"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_dates(dag_run, **context):
    print(f"El DAG Run se ejecutó: {dag_run.start_date}")
    # print(f"Próxima execution_date: {... completar ..  }")


with DAG(
    dag_id="03_scheduling_cada_media_hora",
    default_args={"owner": "airflow"},
    description="Se ejecuta cada 30 minutos",
    # completar
    # completar
    tags=["scheduling", "ejemplos"],
    doc_md=__doc__,
) as dag:

    print_date = PythonOperator(
        task_id="print_execution_date",
        python_callable=_print_dates,
    )
