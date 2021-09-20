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
Example use of Telegram operator.
[source](https://airflow.apache.org/docs/apache-airflow-providers-telegram
/stable/_modules/airflow/providers/telegram/example_dags/example_telegram.html)
"""

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago

dag = DAG(
    "ejemplo_telegram_op",
    start_date=days_ago(2),
    tags=["ejemplos"],
)


enviar_mensaje_telegram = TelegramOperator(
    task_id="enviar_mensaje_telegram",
    telegram_conn_id="telegram_bot_conn",
    chat_id="252499xxxxx",  # cambiar por el chat Id del destinatario
    text="Primer mensaje desde Airflow!",
    dag=dag,
)
