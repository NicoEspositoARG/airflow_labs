"""
#### DAG ejemplo de PythonOperator
Estamos viendo:

- `python_callable`
- `op_kwargs`
- `op_args`
- DebugExecutor

"""

from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "airflow_lab"}

ciudades = ["Roma", "Amsterdam", "Santiago"]


def _saludar():
    print("hola mundo!")


def _saludar_ciudad(**kwargs):
    ciudad_origen = kwargs.get("ciudad", "Buenos Aires")
    print(f"Buen día desde: {ciudad_origen}")


def _saludar_unpacked(ciudad, **kwargs):
    print(f"Buen día desde: {ciudad}")


def _saludar_varios(ciudades, **kwargs):
    for ciudad in ciudades:
        _saludar_unpacked(ciudad)


with DAG(
    dag_id="05_python_op",
    start_date=datetime(2021, 9, 7),
    schedule_interval=None,
    default_args=default_args,
    description="Operator y args",
    catchup=False,
    tags=["ejemplos"],
    doc_md=__doc__,
) as dag:

    saludo = PythonOperator(
        task_id="hola_mundo",
        python_callable=_saludar,
    )

    t0 = PythonOperator(
        task_id="hola_mundo_t0",
        python_callable=_saludar_ciudad,
        op_kwargs={"ciudad": "Londres"},
    )

    t1 = PythonOperator(
        task_id="hola_mundo_t1",
        python_callable=_saludar_unpacked,
        op_kwargs={"ciudad": "Berlin"},  # si lo comentamos?
    )

    t3 = PythonOperator(
        task_id="hola_mundo_t3",
        python_callable=_saludar_varios,
        op_args=[ciudades],
    )


if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
