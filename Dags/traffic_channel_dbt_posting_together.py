from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta

# List of site IDs to run the start_execution function for
sites = [4, 13, 15, 16, 17]

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='traffic_channel_dbt_posting_parallel_together',
    default_args=default_args,
    schedule_interval='30 4 * * *',  # Custom schedule or None
    catchup=False,  # Don't run missed schedules
) as dag:

    @task(execution_timeout=timedelta(minutes=300))  # Timeout set to 300 minutes
    def start_execution_task(site_id):
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_test import start_execution
        # Call the start_execution function for the given site ID
        start_execution(site_id)

    @task
    def start_execution_task_11():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_test import start_execution
        # Call the start_execution function for the given site ID
        start_execution(11)

    @task
    def start_execution_task_14():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_test import start_execution
        # Call the start_execution function for the given site ID
        start_execution(14)

    # Dynamically create tasks for each site in the sites list
    site_tasks = []
    for site in sites:
        # Assign each task to its own execution for parallelism
        task = start_execution_task.override(task_id=f'start_execution_task_{site}')(site)
        site_tasks.append(task)

    # Define task dependencies
    # All site tasks (for 3, 4, 13, 15, 16, 17) should run together
    # Then task for site 11 should run, and after that task 14
    site_tasks >> start_execution_task_14() >> start_execution_task_11()
