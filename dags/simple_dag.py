from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

# def _downloading_data(**kwargs):
#     print(kwargs['ds'])

# def _downloading_data(ds_nodash):
#     print(ds_nodash)

def _downloading_data(my_param, ds):
    print(my_param)


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

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data,
        op_kwargs={'my_param': 42} #passing our own parameters to func _downloading_data
    )
