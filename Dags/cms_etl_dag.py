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
    dag_id='CMS_ETL_Parallel',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Trigger manually or set a schedule
    catchup=False,
) as dag:

    @task
    def site_3_cms():
        # Set the path to the directory containing posting_dbt.py
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_3 import dev_etl
        dev_etl()
    
    @task
    def site_4_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_4 import dev_etl
        dev_etl()

    @task
    def site_10_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_10 import dev_etl
        dev_etl()    
    
    @task
    def site_11_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_11 import dev_etl
        dev_etl()

    @task
    def site_13_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_13 import dev_etl
        dev_etl()

    @task
    def site_14_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_14 import dev_etl
        dev_etl()

    @task
    def site_15_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_15 import dev_etl
        dev_etl()

    @task
    def site_16_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_16 import dev_etl
        dev_etl()

    @task
    def site_17_cms():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_etl_17 import dev_etl
        dev_etl()

    @task
    def etl_data_quality():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from cms_data_quality import check_data_quality
        check_data_quality()

    [site_3_cms(),
     site_4_cms(),
     site_10_cms(),
     site_11_cms(),
     site_13_cms(),
     site_14_cms(),
     site_15_cms(),
     site_16_cms(),
     site_17_cms()
    ] >> etl_data_quality()
