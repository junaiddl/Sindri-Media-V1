from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.utils.dates import days_ago
import os
import sys

default_args = {
    'owner': 'Dot labs',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_quality_check',
    default_args=default_args,
    description='A DAG to run 3 Python scripts in sequential order',
    schedule_interval='30 6 * * *',  # Set to None for manual triggering or define your schedule here
    catchup=False,
) as dag:

    # Define the tasks using the @task decorator
    @task
    def missing_userneeds():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts')
        sys.path.insert(1, script_path)
        print("Checking For Missing Userneeds!")
        from data_quality_missing_userneeds import quality_start
        quality_start()


    @task
    def swagger_posting():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts')
        sys.path.insert(1, script_path)
        print("Checking For Errors In Swagger Posting")
        from data_quality_swagger_posting import quality_start
        quality_start()

    @task
    def traffic_posting():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts')
        sys.path.insert(1, script_path)
        print("Checking For Errors In Traffic Posting")
        from data_quality_traffic_posting import quality_start
        quality_start()

    @task
    def start_data_quality_etl():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from data_quality_etl import run_etl_and_generate_report
        run_etl_and_generate_report()

    # Set the task dependencies for sequential execution
    missing_userneeds() >> swagger_posting() >> traffic_posting() >> start_data_quality_etl()