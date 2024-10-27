from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    'fetch_and_store_danish_energy_data_v4',
    default_args=default_args,
    description='Fetch data from Danish Energy API and store it in PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def fetch_data_from_api():
        API_URL = "https://api.energidataservice.dk/dataset/ElectricitySuppliersPerGridarea?offset=0&sort=Month%20DESC"
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an error if the request fails
        data = response.json().get('records', [])
        return data  # Return data to pass it to the next task

    def store_data_in_postgres(ti):
        data = ti.xcom_pull(task_ids='fetch_data')  # Pull data from the previous task
        if not data:
            raise ValueError("No data was fetched from the API.")

        # Initialize PostgreSQL hook
        postgres_hook = PostgresHook(postgres_conn_id='Danish_connection')  # Replace with your connection ID

        # Create the table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Danish (
            Month DATE,
            GridCompany VARCHAR(50),
            ActiveSupplierPerGridArea VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

        # Insert data into the table
        for record in data:
            insert_query = f"""
            INSERT INTO Danish (Month, GridCompany, ActiveSupplierPerGridArea)
            VALUES (
                '{record['Month']}',
                '{record['GridCompany']}',
                '{record['ActiveSupplierPerGridArea']}'
            ) ON CONFLICT DO NOTHING;
            """
            postgres_hook.run(insert_query)

    # Define tasks
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=store_data_in_postgres
    )

    # Set task dependencies
    fetch_data >> store_data
