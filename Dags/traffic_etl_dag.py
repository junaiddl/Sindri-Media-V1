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
    dag_id='Traffic_ETL_Parallel',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Trigger manually or set a schedule
    catchup=False,
) as dag:

    @task
    def site_4_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_4 import dev_etl
        dev_etl()

    @task
    def site_10_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_10 import dev_etl
        dev_etl()    
    
    @task
    def site_11_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_11 import dev_etl
        dev_etl()

    @task
    def site_13_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_13 import dev_etl
        dev_etl()

    @task
    def site_14_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_14 import dev_etl
        dev_etl()

    @task
    def site_15_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_15 import dev_etl
        dev_etl()

    @task
    def site_16_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_16 import dev_etl
        dev_etl()

    @task
    def site_17_traffic():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_etl_17 import dev_etl
        dev_etl()

    @task
    def etl_data_quality():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from traffic_data_quality import check_data_quality
        check_data_quality()

    site_4_traffic() >> site_10_traffic() >> site_11_traffic() >> site_13_traffic() >> site_14_traffic() >> site_15_traffic() >> site_16_traffic() >> site_17_traffic() >> etl_data_quality()
