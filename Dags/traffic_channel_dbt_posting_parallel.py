from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta

# List of site IDs to run the start_execution function for
sites = [4, 11, 13, 14, 15, 16, 17]
# sites = [15]

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='traffic_channel_dbt_posting_parallel',
    default_args=default_args,
    schedule_interval='30 3 * * *',  # No regular schedule, trigger manually
    catchup=False,  # Don't run missed schedules
) as dag:

    @task(execution_timeout=timedelta(minutes=300))  # Timeout set to 300 minutes
    def start_execution_task(site_id):
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_Parallel import start_execution
        # Call the start_execution function for the given site ID
        start_execution(site_id)

    # Dynamically create tasks for each site in the sites list
    for site in sites:
        # Assign each task to its own execution for parallelism
        # Here, we assign a unique task_id that includes the site_id
        start_execution_task.override(task_id=f'start_execution_task_{site}')(site)
