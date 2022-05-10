from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

# def _downloading_data(**kwargs):
#     print(kwargs['ds'])

# def _downloading_data(ds_nodash):
#     print(ds_nodash)

# def _downloading_data(my_param, ds):
#     print(my_param)

def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')


def _checking_data():
    print('checking data')


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
        python_callable=_downloading_data
        # op_kwargs={'my_param': 42} #passing our own parameters to func _downloading_data
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default', #create it on the ui
        filepath='my_file.txt'
        # ,poke_interval=30 #how often should the sensor check for the file
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0'
    )

    # downloading_data.set_downstream(waiting_for_data)
    # waiting_for_data.set_downstream(processing_data)

    # processing_data.set_upstream(waiting_for_data)
    # waiting_for_data.set_upstream(downloading_data)

    # downloading_data >> waiting_for_data >> processing_data

    # downloading_data >> [waiting_for_data >> processing_data]

    chain(downloading_data, waiting_for_data, checking_data, processing_data)
    # cross_downstream([downloading_data, checking_data], [waiting_for_data, processing_data])
