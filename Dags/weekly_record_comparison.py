from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta
import os
import sys

# Default arguments for the DAG
default_args = {
    'owner': 'Team Sindri Media',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Weekly_Record_Comparison',
    default_args=default_args,
    schedule_interval='2 1 * * 5',  # Trigger manually or set a schedule
    catchup=True,
) as dag:

    @task
    def site_4_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_4 import dev_etl
        dev_etl()

    @task
    def site_10_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_10 import dev_etl
        dev_etl()    
    
    @task
    def site_11_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_11 import dev_etl
        dev_etl()

    @task
    def site_13_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_13 import dev_etl
        dev_etl()

    @task
    def site_14_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_14 import dev_etl
        dev_etl()

    @task
    def site_15_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_15 import dev_etl
        dev_etl()

    @task
    def site_16_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_16 import dev_etl
        dev_etl()

    @task
    def site_17_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_17 import dev_etl
        dev_etl()

    @task
    def site_18_compare():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare_18 import dev_etl
        dev_etl()

    @task
    def compile_email():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Data_Quality', 'Record_Comparison')
        sys.path.insert(1, script_path)
        from data_quality_site_archive_weekly_compare import dev_etl
        dev_etl()

    site_4_compare() >> site_10_compare() >> site_11_compare() >> site_13_compare() >> site_14_compare() >> site_15_compare() >> site_16_compare() >> site_17_compare() >> site_18_compare() >>compile_email()
