from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta

default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

with DAG(
    dag_id='traffic_channel_dbt_posting',
    default_args=default_args,
    schedule_interval='30 3 * * *',
    catchup=False,
) as dag:

    @task(execution_timeout=timedelta(minutes=300))  # Timeout set to 300 minutes
    def start_execution_task():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting import start_execution
        start_execution()

    start_execution_task()
