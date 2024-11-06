from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

with DAG(
    'refresh_views_daily',
    default_args=default_args,
    description='DAG to refresh PostgreSQL views daily',
    schedule_interval=timedelta(days=1),  # Refresh daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Refresh daily price trends view
    refresh_daily_price_trends = PostgresOperator(
        task_id="refresh_daily_price_trends",
        postgres_conn_id="Danish_Airflow",
        sql="""
        CREATE OR REPLACE VIEW daily_price_trends AS
        SELECT 
            DATE(HourDK) AS recorded_day,
            PriceArea,
            AVG(aFRR_DownCapPriceDKK) AS avg_down_price_dkk,
            AVG(aFRR_UpCapPriceDKK) AS avg_up_price_dkk,
            AVG(aFRR_DownCapPriceEUR) AS avg_down_price_eur,
            AVG(aFRR_UpCapPriceEUR) AS avg_up_price_eur
        FROM 
            afrr_reserves
        GROUP BY 
            DATE(HourDK), PriceArea;
        """
    )

    # Refresh daily reserve demand view
    refresh_daily_reserve_demand = PostgresOperator(
        task_id="refresh_daily_reserve_demand",
        postgres_conn_id="Danish_Airflow",
        sql="""
        CREATE OR REPLACE VIEW daily_reserve_demand AS
        SELECT 
            DATE(HourDK) AS recorded_day,
            PriceArea,
            SUM(aFRR_DownPurchased) AS total_down_reserve,
            SUM(aFRR_UpPurchased) AS total_up_reserve
        FROM 
            afrr_reserves
        GROUP BY 
            DATE(HourDK), PriceArea;
        """
    )

    # Refresh market efficiency metrics view
    refresh_market_efficiency = PostgresOperator(
        task_id="refresh_market_efficiency",
        postgres_conn_id="Danish_connection",
        sql="""
        CREATE OR REPLACE VIEW market_efficiency AS
        SELECT 
            PriceArea,
            MIN(aFRR_UpCapPriceDKK) AS min_up_price_dkk,
            MAX(aFRR_UpCapPriceDKK) AS max_up_price_dkk,
            AVG(aFRR_UpCapPriceDKK) AS avg_up_price_dkk,
            MIN(aFRR_DownCapPriceDKK) AS min_down_price_dkk,
            MAX(aFRR_DownCapPriceDKK) AS max_down_price_dkk,
            AVG(aFRR_DownCapPriceDKK) AS avg_down_price_dkk
        FROM 
            afrr_reserves
        GROUP BY 
            PriceArea;
        """
    )

    # Refresh regional demand comparison view
    refresh_regional_demand_comparison = PostgresOperator(
        task_id="refresh_regional_demand_comparison",
        postgres_conn_id="Danish_connection",
        sql="""
        CREATE OR REPLACE VIEW regional_demand_comparison AS
        SELECT 
            PriceArea,
            AVG(aFRR_UpPurchased) AS avg_up_reserve,
            AVG(aFRR_DownPurchased) AS avg_down_reserve
        FROM 
            afrr_reserves
        GROUP BY 
            PriceArea;
        """
    )

    [refresh_daily_price_trends, refresh_daily_reserve_demand, refresh_market_efficiency, refresh_regional_demand_comparison]
