from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="air_pollution_dag",
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2)
    }
)
def air_pollution_dag():
    
    start = EmptyOperator(task_id="start")

    check_api = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="weathermap_api",
        endpoint="/data/2.5/air_pollution/history",
        response_check=lambda response: response.status_code in [200, 401, 403]
    )

    @task 
    def extract_data():
        pass
