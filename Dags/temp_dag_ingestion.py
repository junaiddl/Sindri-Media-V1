from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import os
import sys
import importlib

traffic_sites = [19]

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,  # No retries
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Data_Ingestion_V2',
    default_args=default_args,
    schedule_interval='15 2 * * *',  # Daily at midnight
    catchup=False,
) as dag:

    @task_group(group_id="traffic_etls")
    def traffic_etl_group():
        previous_task = None
        for site_id in traffic_sites:
            @task(task_id=f"site_{site_id}_traffic", execution_timeout=timedelta(minutes=300))
            def traffic_etl():
                script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
                sys.path.insert(1, script_path)
                module_name = f"traffic_etl_{site_id}"
                try:
                    module = importlib.import_module(module_name)
                    module.dev_etl()
                except ImportError as e:
                    raise ImportError(f"Failed to import module {module_name}: {e}")
            current_task = traffic_etl()

            # Chain tasks sequentially
            if previous_task:
                previous_task >> current_task
            previous_task = current_task

    traffic_etls = traffic_etl_group()

    traffic_etls