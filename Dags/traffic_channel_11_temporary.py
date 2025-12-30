from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta


# Default arguments for the DAG
default_args = {
    'owner': 'Ashhar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='traffic_channel_11_temporary',
    default_args=default_args,
    schedule_interval='30 5 * * *',  # No regular schedule, trigger manually
    catchup=False,  # Don't run missed schedules
) as dag:

    @task
    def insert_into_tabels():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel', '11_Bug_Temporary_Solution')
        sys.path.insert(1, script_path)
        from Traffic_Channels_11_Yearly_Table import start_insert
        # Call the start_execution function for the given site ID
        start_insert()

    @task
    def post_week_month():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel', '11_Bug_Temporary_Solution')
        sys.path.insert(1, script_path)
        from Traffic_Channels_11_Other_Posting import start_execution
        # Call the start_execution function for the given site ID
        start_execution(11)

    @task
    def post_year():
        import os
        import sys
        script_path = os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting', 'Traffic_Channel', '11_Bug_Temporary_Solution')
        sys.path.insert(1, script_path)
        from Traffic_Channels_11_Yearly_Posting import start_execution
        # Call the start_execution function for the given site ID
        start_execution(11)

    insert_into_tabels() >> post_week_month() >> post_year()