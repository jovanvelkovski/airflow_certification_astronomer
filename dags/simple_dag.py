from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        # start_date=datetime(2022, 1, 1),
        # schedule_interval="*/10 * * * *"
        # schedule_interval="@weekly"
        # schedule_interval=None
        # schedule_interval=timedelta(days=3)
        # max_active_runs=3
        dag_id='simple_dag',
        default_args=default_args,
        start_date=days_ago(3),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    task_1 = DummyOperator(
        task_id='task_1'
    )

    task_2 = DummyOperator(
        task_id='task_2'
    )

    task_3 = DummyOperator(
        task_id='task_3'
    )
