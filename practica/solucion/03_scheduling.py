"""DAG ejercitación schedule_interval & DAG Runs"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_dates(dag_run, **context):
    print(f"El DAG Run se ejecutó: {dag_run.start_date}")
    print(f"Próxima execution_date: {context['next_execution_date']}")


with DAG(
    dag_id="03_scheduling_cada_media_hora",
    default_args={"owner": "airflow"},
    description="Se ejecuta cada 30 minutos",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2021, 9, 21, 8, 0),
    tags=["scheduling", "ejemplos"],
    doc_md=__doc__,
) as dag:

    print_date = PythonOperator(
        task_id="print_execution_date",
        python_callable=_print_dates,
    )

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
