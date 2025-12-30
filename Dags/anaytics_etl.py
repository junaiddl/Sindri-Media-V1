from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta
import os
import sys

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Analytics_ETL',
    default_args=default_args,
    schedule_interval='45 0 * * *',  # Trigger manually or set a schedule
    catchup=False,
) as dag:
    
    script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL', 'Analytics')
    sys.path.insert(1, script_path)
    from analytics_etl_v2 import dev_etl

    @task
    def site_3_analytics():
        dev_etl(3)
    
    @task
    def site_4_analytics():
        dev_etl(4)

    @task
    def site_10_analytics():
        dev_etl(10)    
    
    @task
    def site_11_analytics():
        dev_etl(11)

    @task
    def site_13_analytics():
        dev_etl(13)

    @task
    def site_14_analytics():
        dev_etl(14)

    @task
    def site_15_analytics():
        dev_etl(15)

    @task
    def site_16_analytics():
        dev_etl(16)

    @task
    def site_17_analytics():
        dev_etl(17)

    site_3_analytics() >> site_4_analytics() >> site_10_analytics() >> site_11_analytics() >> site_13_analytics() >> site_14_analytics() >> site_15_analytics() >> site_16_analytics() >> site_17_analytics()