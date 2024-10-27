from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_danish_energy_data',
    default_args=default_args,
    description='Fetch data from Danish Energy API and print to terminal',
    schedule_interval=timedelta(days=1),  # Change as desired
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def fetch_data_from_api():
        API_URL = "https://api.energidataservice.dk/dataset/ElectricitySuppliersPerGridarea?offset=0&sort=Month%20DESC"
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an error if the request fails
        data = response.json().get('records', [])
        print("Fetched Data:", data)  # Print to Airflow logs for verification

    # Define the task
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )
