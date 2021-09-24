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

"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def _data_from_mysql():
    # fetch data from the DB or anywhere else
    # set a Variable
    data = {'legion': {'company': 'some_company', 'laptop': 'great_laptop'}}
    Variable.set('jobconfig', data, serialize_json=True)


dag = DAG(
    dag_id="example_trigger_controller_dag",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval="@once",
    tags=['example','depdendencias'],
)

get_data_from_MySql = PythonOperator(
    task_id='get_data_from_MySql',
    python_callable=_data_from_mysql,
)

trigger = TriggerDagRunOperator(
    task_id="test_trigger_dagrun",
    # Ensure this equals the dag_id of the DAG to trigger
    trigger_dag_id="example_trigger_target_dag",
    conf={"message": "Company is {{var.json.jobconfig.legion.company}}"},
    execution_date='{{ds}}',
    dag=dag,
)
get_data_from_MySql >> trigger
