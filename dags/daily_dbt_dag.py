from airflow.sdk import dag, task, Variable
from pendulum import datetime, duration
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import pandas as pd
import logging
@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(minutes=2),
    },
    tags=["sales_pipeline"],
)
def dbt_build():
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /usr/local/airflow/ass_3 && dbt build --select tag:daily --profiles-dir /usr/local/airflow/ass_3 --project-dir /usr/local/airflow/ass_3'
    )
    run_dbt




dbt_build()