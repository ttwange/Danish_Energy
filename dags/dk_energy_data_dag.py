from airflow import DAG
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_dalay': timedelta(minutes=5),
}

dag = DAG(
    'fetch and store Danish energy data',
    default_args=default_args,
    description='A simple DAG to pull data from Danish API and load it into PostgreSQL',
    schedule_interval=timedelta(days=1),
)