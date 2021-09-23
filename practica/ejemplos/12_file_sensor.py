"""DAG ejemplo FileSensor"""

from datetime import timedelta, datetime

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="12_file_sensor",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=datetime(2021, 9, 7),
    tags=["sensors", "ejemplos"],
    doc_md=__doc__,
) as dag:

    archivo_existente = FileSensor(
        task_id="archivo_existente",
        filepath="/home/vagrant/dataset_buscado.csv"
        # fs_conn_id='fs_default'
    )

    fin = DummyOperator(
        task_id="fin",
    )
    archivo_existente >> fin
