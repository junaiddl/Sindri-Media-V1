from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),  # Adjust the start date as needed
    'retries': 1,
}

with DAG(
    dag_id='Weekly_Logs_Removal',
    default_args=default_args,
    schedule_interval='0 10 * * 5',
    catchup=False,
) as dag:

    @task()
    def start_execution_task():
        import os
        import sys        
        # Set the path to the directory containing posting_dbt.py
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts')
        sys.path.insert(1, script_path)
        from airflow_logs_removal import main_function
        # Call the start_execution function
        main_function()

    # Define the task
    start_execution_task()