from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models.xcom_arg import XComArg


default_args = {"start_date": days_ago(2)}


def _fetch_data():
    return "data from somewhere else"


def print_data(data_arg, **context):

    print(f"Data obtained from previous task: {data_arg } ")


with DAG(
    "xcomarg_example",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=_fetch_data,
    )

    print_data = PythonOperator(
        task_id="print_data",
        python_callable=print_data,
        op_kwargs={"data_arg": fetch_data.output},
        # op_kwargs={"data_arg": "{{ ti.xcom_pull(task_ids='fetch_data') }}"},
    )
    # fetch_data >> print_data

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
