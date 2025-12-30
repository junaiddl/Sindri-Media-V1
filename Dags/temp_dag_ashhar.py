from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from datetime import timedelta
import os
import sys

# Set up the script paths once at the top
dbt_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting')
traffic_path = os.path.join(dbt_path, 'Traffic_Channel')
sys.path.insert(1, dbt_path)
sys.path.insert(1, traffic_path)

# List of site IDs
traffic_sites = [18]
swagger_sites = [18]

# Default arguments for the DAG
default_args = {
    'owner': 'Dot labs',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='Temp_DAG',
    default_args=default_args,
    schedule_interval='30 3 * * *',  # DAG will only run when triggered manually
    catchup=False, 
) as dag:

   # Group Swagger Tasks
    @task_group(group_id="swagger_tasks")
    def swagger_task_group():
        for site_id in swagger_sites:
            def create_swagger_task(site_id):
                @task(
                    execution_timeout=timedelta(minutes=300),
                    task_id=f'start_execution_task_swagger_{site_id}',
                )
                def start_execution_task_swagger():
                    from posting_dbt_parallel import execute
                    execute(site_id)

                return start_execution_task_swagger()

            create_swagger_task(site_id)

# Group Traffic Tasks
    @task_group(group_id="traffic_tasks")
    def traffic_task_group():
        for site_id in traffic_sites:
            def create_traffic_task(site_id):
                @task(
                    execution_timeout=timedelta(minutes=300),
                    task_id=f'start_execution_task_traffic_{site_id}',
                )
                def start_execution_task_traffic():
                    from Traffic_Channels_Posting_test import start_execution
                    start_execution(site_id)

                return start_execution_task_traffic()

            create_traffic_task(site_id)

    # Instantiate task groups and individual tasks
    swagger_tasks = swagger_task_group()
    traffic_tasks = traffic_task_group()

    # Define task dependencies
    swagger_tasks >> traffic_tasks
