from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from include.air_pollution.tasks.extract import get_cities_config
from airflow.models.variable import Variable
import pendulum

cities_to_process = get_cities_config()



@dag(
    dag_id="air_pollution_dag",
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1)
    }
)
def air_pollution_dag():
    
    start = EmptyOperator(task_id="start")

    check_api = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="weathermap_api",
        endpoint="/data/2.5/air_pollution/history",
        response_check=lambda response: response.status_code in [200, 401],
        extra_options={'check_response': False}
    )

    @task
    def get_current_timestamps_range():
        start_ts = int(Variable.get('air_pollution_last_ts'))
        end_ts = int(pendulum.now(tz='UTC').timestamp())

        time_range = {'start_ts': start_ts, 'end_ts': end_ts}
        return time_range

    @task
    def extract_data(city_info: dict, time_range: dict, logical_date):
        from include.air_pollution.tasks.extract import extract_air_pollution_data

        print(f"GOT time_range: {time_range}")

        s3_key_to_raw_data = extract_air_pollution_data(
            city=city_info['city'],
            lat=city_info['lat'],
            lon=city_info['lon'],
            start_ts=time_range['start_ts'],
            end_ts=time_range['end_ts'],
            logical_date=logical_date
            )
        
        return {'s3_key_to_raw_data': s3_key_to_raw_data, 'city': city_info['city']}
    
    @task
    def transform_data(extract_output: dict, logical_date):
        from include.air_pollution.tasks.transform import transform_air_pollution_data
        s3_key_to_raw_data = extract_output['s3_key_to_raw_data']
        city = extract_output['city']

        s3_key_to_transformed_data = transform_air_pollution_data(
            s3_key=s3_key_to_raw_data,
            city=city,
            logical_date=logical_date
        )

        return s3_key_to_transformed_data
        

    @task
    def update_last_timestamp(time_range: dict):
        new_ts = time_range['end_ts']
        Variable.set('air_pollution_last_ts', str(new_ts))
        print(f"Last timestamp updated to: {new_ts}")

    time_range_task_output = get_current_timestamps_range()

    extract_tasks_group = extract_data.partial(time_range=time_range_task_output).expand(city_info=cities_to_process)

    transform_tasks_group = transform_data.expand(extract_output=extract_tasks_group)

    update_timestamp_task = update_last_timestamp(
        time_range=time_range_task_output
    )

    start >> check_api >> time_range_task_output >> extract_tasks_group >> transform_tasks_group >> update_timestamp_task

air_pollution_dag()



