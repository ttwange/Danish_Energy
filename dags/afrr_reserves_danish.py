from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_transform_store_danish_afrr_data',
    default_args=default_args,
    description='Fetch, transform, and store Danish aFRR data into PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def fetch_data_from_api():
        today_date = datetime.now().strftime("%Y-%m-%dT00:00:00")
        
        API_URL = "https://api.energidataservice.dk/dataset/AfrrReservesNordic?offset=0&start=2024-07-26T00:00&end{today_date}&sort=HourDK%20DESC"
        #API_URL = "https://api.energidataservice.dk/dataset/AfrrReservesNordic?offset=0&start=2024-07-26T00:00&end=2024-10-27T00:00&sort=HourDK%20DESC"
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an error if the request fails
        data = response.json().get('records', [])
        return data  # Return data to pass it to the next task

    def transform_data(ti):
        data = ti.xcom_pull(task_ids='fetch_data')  # Pull data from the previous task
        if not data:
            raise ValueError("No data was fetched from the API.")

        # Transform the data: drop 'HourUTC' from each record
        transformed_data = [
            {k: v for k, v in record.items() if k != 'HourUTC'}
            for record in data
        ]

        # Push transformed data to XCom for the next task
        ti.xcom_push(key='transformed_data', value=transformed_data)

    def store_data_in_postgres(ti):
        # Pull transformed data from XCom
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        if not transformed_data:
            raise ValueError("No transformed data was found.")

        # Initialize PostgreSQL hook
        postgres_hook = PostgresHook(postgres_conn_id='Danish_Airflow')  # Replace with your connection ID

        # Define the table structure
        create_table_query = """
        CREATE TABLE IF NOT EXISTS afrr_reserves (
            HourDK TIMESTAMP,
            PriceArea VARCHAR(50),
            aFRR_DownPurchased FLOAT,
            aFRR_UpPurchased FLOAT,
            aFRR_DownCapPriceDKK FLOAT,
            aFRR_DownCapPriceEUR FLOAT,
            aFRR_UpCapPriceDKK FLOAT,
            aFRR_UpCapPriceEUR FLOAT
        );
        """
        postgres_hook.run(create_table_query)

        # Insert data into the table
        for record in transformed_data:
            insert_query = f"""
            INSERT INTO afrr_reserves (HourDK, PriceArea, aFRR_DownPurchased, aFRR_UpPurchased, aFRR_DownCapPriceDKK,
                                        aFRR_DownCapPriceEUR, aFRR_UpCapPriceDKK, aFRR_UpCapPriceEUR)
            VALUES (
                '{record['HourDK']}',
                '{record['PriceArea']}',
                {record.get('aFRR_DownPurchased', 'NULL')},
                {record.get('aFRR_UpPurchased', 'NULL')},
                {record.get('aFRR_DownCapPriceDKK', 'NULL')},
                {record.get('aFRR_DownCapPriceEUR', 'NULL')},
                {record.get('aFRR_UpCapPriceDKK', 'NULL')},
                {record.get('aFRR_UpCapPriceEUR', 'NULL')}
            ) ON CONFLICT DO NOTHING;
            """
            postgres_hook.run(insert_query)

    # Define tasks
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=store_data_in_postgres
    )

    # Set task dependencies
    fetch_data >> transform_data >> store_data
