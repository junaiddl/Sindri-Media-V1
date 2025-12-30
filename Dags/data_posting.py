from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta

# List of site IDs to run the start_execution function for
traffic_sites = [4, 13, 15, 16, 17]
swagger_sites = [3, 4, 10, 11, 13, 14, 15, 16, 17]

# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='Data_Posting',
    default_args=default_args,
    schedule_interval='30 1 * * *',  # Custom schedule or None
    catchup=False,  # Don't run missed schedules
) as dag:

    # Task factory for Swagger
    def start_swagger(site_id):
        @task(
            execution_timeout=timedelta(minutes=300),  # Timeout set to 300 minutes
            task_id=f'start_execution_task_swagger_{site_id}',  # Dynamically generate task_id
        )
        def start_execution_task_swagger():
            import os
            import sys
            script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting')
            sys.path.insert(1, script_path)
            from posting_dbt_parallel import execute
            # Call the start_execution function for the given site ID
            execute(site_id)
        return start_execution_task_swagger

    # Task factory for Traffic
    def start_traffic(site_id):
        @task(
            execution_timeout=timedelta(minutes=300),  # Timeout set to 300 minutes
            task_id=f'start_execution_task_traffic_{site_id}',  # Dynamically generate task_id
        )
        def start_execution_task_traffic():
            import os
            import sys
            script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
            sys.path.insert(1, script_path)
            from Traffic_Channels_Posting_test import start_execution
            # Call the start_execution function for the given site ID
            start_execution(site_id)
        return start_execution_task_traffic

    @task()
    def start_execution_task_11():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_test import start_execution
        # Call the start_execution function for the given site ID
        start_execution(11)

    @task()
    def start_execution_task_14():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel')
        sys.path.insert(1, script_path)
        from Traffic_Channels_Posting_test import start_execution
        # Call the start_execution function for the given site ID
        start_execution(14)

    # Create Swagger and Traffic tasks
    swagger_tasks = [start_swagger(site_id)() for site_id in swagger_sites]
    traffic_tasks = [start_traffic(site_id)() for site_id in traffic_sites]

    traffic_task_11 = start_execution_task_11()
    traffic_task_14 = start_execution_task_14()

    # Set task dependencies
    # All Swagger tasks run in parallel
    # for i in range(len(swagger_tasks) - 1):
    #     swagger_tasks[i] >> swagger_tasks[i + 1]

    # All Swagger tasks must complete before any Traffic task starts
    for swagger_task in swagger_tasks:
        for traffic_task in traffic_tasks:
            swagger_task >> traffic_task

    # All Traffic tasks must complete before task 11 runs
    for traffic_task in traffic_tasks:
        traffic_task >> traffic_task_14

    # Task 11 must complete before task 14 runs
    traffic_task_14 >> traffic_task_11
