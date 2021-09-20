"""
### Este DAG procesa los tickets diarios.
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
    dag_id="copia_dag_params",  # renombar a " ejemplo_parametros_de_un_dag"
    start_date=datetime(2021, 9, 7),
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
