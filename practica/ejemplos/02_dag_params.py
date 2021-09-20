"""
### Este DAG procesa los tickets cada 8 hs.
cuerpo del mensaje
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "mi_empresa",
    "params": {
        "prioridad": "alta",
    },
}

with DAG(
    dag_id="02_dag_params",
    start_date=datetime(2021, 9, 7),
    schedule_interval="0 */8 * * *",
    default_args=default_args,
    description="una fantástica descripción",
    tags=["ejemplos"],
    doc_md=__doc__,
) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "recibi prioridad: {{ params.prioridad }}"',
        doc_md="""\
                #Inicio
                Primer tarea de la etapa.
                """,
    )
