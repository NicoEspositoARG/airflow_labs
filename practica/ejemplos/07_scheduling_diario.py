"""DAG ejemplo schedule_interval & DAG Runs"""

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _print_date(dag_run, **context):
    print(f"El DAG Run se ejecutó: {dag_run.start_date}")


with DAG(
    dag_id="07_scheduling_diario",
    default_args={"owner": "airflow"},
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 9, 7),
    tags=["scheduling", "ejemplos"],
    doc_md=__doc__,
) as dag:

    print_date = PythonOperator(
        task_id="print_execution_date",
        python_callable=_print_date,
    )

    for i in range(2):
        task = BashOperator(
            task_id="task_nro_" + str(i),
            bash_command='echo next execution_date: "{{ next_execution_date }}" && sleep 1',
        )


if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
