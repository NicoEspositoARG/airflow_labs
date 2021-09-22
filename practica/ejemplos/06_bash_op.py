#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    "owner": "airflow",
}
docs = """
"Basado en el ejemplo original de Airflow"

[Default Variables docs reference](https://airflow.apache.org/
docs/apache-airflow/stable/macros-ref.html).
"""

with DAG(
    dag_id="06_bash_op",
    default_args=args,
    schedule_interval="0 0 * * *",  # daily
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example", "ejemplos"],
    params={"example_key": "example_value"},
    doc_md=docs,
) as dag:

    run_this_last = DummyOperator(
        task_id="run_this_last",
    )

    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id="runme_" + str(i),
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )
    also_run_this >> run_this_last

this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
this_will_skip >> run_this_last

os_date_task = BashOperator(
    task_id="date_os",
    bash_command='echo "La fecha del SO es: " $(date);',
    dag=dag,
)
