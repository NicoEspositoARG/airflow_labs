"""
#### DAG de ejemplo del ciclo de vida de las tareas.
Estamos viendo:

- Estados por los que pasan las Task durante la ejecución de un DAG.
- UI: Task Actions
- UI: Gantt, Task Tries, Landing Times
- `execution_timeout`
- `on_failure_callback`
- `on_execute_callback`
- `execute`
- `retries`
- `retry_delay`

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "mi_empresa",
    "params": {
        "prioridad": "alta",
    },
}


def realizar_antes(context):
    print("****Antes de empezar..***")


def sonar_alarma(context):
    task_instance: TaskInstance = context.get("task_instance")
    print("******* Alarma alarma!! ***************")
    print(f"Task Instance: {task_instance}")
    duracion = timedelta(seconds=round(task_instance.duration))
    print(f"La tarea duró: {duracion} segundos")


with DAG(
    dag_id="04_ciclo_de_vida",
    start_date=datetime(2021, 9, 7),
    schedule_interval="@monthly",
    default_args=default_args,
    description="Tasks - ciclo de vida y callbacks",
    catchup=False,
    tags=["ejemplos"],
    doc_md=__doc__,
) as dag:
    demora = BashOperator(
        task_id="demora",
        bash_command='echo "esperando..";sleep 30; echo "Terminó!!"',
    )
    fallara = BashOperator(
        task_id="fallara",
        bash_command="exit 1",
        on_failure_callback=sonar_alarma,
    )
    demora_demasiado = BashOperator(
        task_id="demora_demasiado",
        bash_command='echo "esperando..";sleep 60; echo "Terminó!!"',
        execution_timeout=timedelta(seconds=30),
        on_failure_callback=sonar_alarma,
    )
    optimista = BashOperator(
        task_id="optimista",
        bash_command="exit 1",
        retries=2,
        retry_delay=timedelta(seconds=20),
    )

    inicio = DummyOperator(task_id="inicio")
    exitosa = DummyOperator(task_id="exitosa")

    falla_el_upstream = DummyOperator(task_id="falla_el_upstream")
    multiples_upstreams = BashOperator(
        task_id="multiples_upstreams",
        bash_command="echo 'éxito!' ",
    )

    procesando = BashOperator(
        task_id="procesando",
        bash_command='echo "estamos procesando.."',
        on_execute_callback=realizar_antes,
    )

    inicio >> demora >> demora_demasiado >> falla_el_upstream
    inicio >> exitosa >> [fallara, procesando] >> multiples_upstreams
    inicio >> optimista
