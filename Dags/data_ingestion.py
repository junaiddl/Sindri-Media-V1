from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta
import os
import sys
import importlib

cms_sites = [4, 10, 11, 13, 14, 15, 16, 17]
analytics_sites = [4, 10, 11, 13, 14, 15, 16, 17]
traffic_sites = [4, 11, 13, 14, 15, 16, 17]

# Default arguments for the DAG
default_args = {
    'owner': 'Dot labs',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,  # No retries
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Data_Ingestion',
    default_args=default_args,
    schedule_interval='15 2 * * *',  # Daily at midnight
    catchup=False,
) as dag:

    def start_cms(site_id):
        @task(task_id=f"site_{site_id}_cms", execution_timeout=timedelta(minutes=300))
        def cms_etl():
            script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
            sys.path.insert(1, script_path)
            module_name = f"cms_etl_{site_id}"
            try:
                module = importlib.import_module(module_name)
                module.dev_etl()
            except ImportError as e:
                raise ImportError(f"Failed to import module {module_name}: {e}")
        return cms_etl

    def start_analytics(site_id):
        @task(task_id=f"site_{site_id}_analytics", execution_timeout=timedelta(minutes=300))
        def analytics_etl():
            script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL', 'Analytics')
            sys.path.insert(1, script_path)
            from analytics_etl_v2 import dev_etl
            dev_etl(site_id)
        return analytics_etl

    def start_traffic(site_id):
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
        return traffic_etl

    # @task
    # def etl_data_quality_cms():
    #     script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
    #     sys.path.insert(1, script_path)
    #     from cms_data_quality import check_data_quality
    #     check_data_quality()

    # @task
    # def etl_data_quality_traffic():
    #     script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
    #     sys.path.insert(1, script_path)
    #     from traffic_data_quality import check_data_quality
    #     check_data_quality()

    @task
    def start_userneeds():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'ETL')
        sys.path.insert(1, script_path)
        from userneeds_data import dev_etl
        dev_etl()

    # Dynamically create CMS tasks
    cms_tasks = [start_cms(site_id)() for site_id in cms_sites]
    analytics_tasks = [start_analytics(site_id)() for site_id in analytics_sites]
    traffic_tasks = [start_traffic(site_id)() for site_id in traffic_sites]


    cms_sequence = cms_tasks[0]
    for task in cms_tasks[1:]:
        cms_sequence >> task
        cms_sequence = task

    cms_sequence  >> analytics_tasks[0] >> analytics_tasks[1] >> analytics_tasks[2] >> analytics_tasks[3] >> analytics_tasks[4] >> analytics_tasks[5] >> analytics_tasks[6] >> analytics_tasks[7] >> analytics_tasks[8]

    # # Dynamically create Analytics tasks
    # analytics_sequence = analytics_tasks[0]
    # for task in analytics_tasks[1:]:
    #     analytics_sequence >> task
    #     analytics_sequence = task

    # etl_data_quality_cms() >> analytics_sequence

    # # Dynamically create Traffic tasks
    # traffic_tasks = [start_traffic(site_id)() for site_id in traffic_sites]
    traffic_sequence = traffic_tasks[0]
    for task in traffic_tasks[1:]:
        traffic_sequence >> task
        traffic_sequence = task

    analytics_tasks[8] >> traffic_tasks[0]
    traffic_tasks[-1] >> start_userneeds()

    # analytics_sequence >> traffic_sequence
    # traffic_sequence >> etl_data_quality_traffic()
    # etl_data_quality_traffic() >> start_userneeds()
