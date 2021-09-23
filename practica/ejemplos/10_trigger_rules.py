"""
[Source](https://airflow.apache.org
/docs/apache-airflow/stable/concepts/dags.html#trigger-rules)
"""
import datetime as dt

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="branch_without_trigger",
    schedule_interval="@once",
    start_date=dt.datetime(2019, 2, 28),
)

run_this_first = DummyOperator(task_id="run_this_first", dag=dag)
branching = BranchPythonOperator(
    task_id="branching", dag=dag, python_callable=lambda: "branch_a"
)

branch_a = DummyOperator(task_id="branch_a", dag=dag)
follow_branch_a = DummyOperator(task_id="follow_branch_a", dag=dag)

branch_false = DummyOperator(task_id="branch_false", dag=dag)

join = DummyOperator(task_id="join", dag=dag)
#  trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED

run_this_first >> branching
branching >> branch_a >> follow_branch_a >> join
branching >> branch_false >> join
