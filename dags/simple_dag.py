from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'jovan@mail.com'
}


def _failure(context):
    print('On callback failure')
    print(context)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
)
def simple_dag():
    @task()
    def downloading_data():
        with open('/tmp/my_file.txt', 'w') as f:
            f.write('my_data')
        return {"my_key": 43}

    @task()
    def checking_data(my_keys: dict):
        my_xcom = my_keys["my_key"]
        print(my_xcom)

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt'
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0',
        on_failure_callback=_failure
    )

    downloaded_data = downloading_data()
    checked_data = checking_data(downloaded_data)

    chain(downloaded_data, checked_data, waiting_for_data, processing_data)


simple_dag = simple_dag()
