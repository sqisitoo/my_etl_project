from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task

from plugins.common.config import settings

PLUGINS_DIR = "/opt/airflow/plugins"


@dag(
    dag_id="air_pollution_dag",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath=[PLUGINS_DIR],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def air_pollution_dag():
    @task
    def get_cities_config():
        from plugins.common.config.cities import get_cities_config

        config_obj = get_cities_config()

        return [city.model_dump() for city in config_obj.cities]

    init_table = SQLExecuteQueryOperator(
        task_id="init_schema", sql="pipelines/air_pollution/sql/init_schema.sql", conn_id="my_postgres_dwh"
    )

    @task
    def extract_data(city_info: dict, logical_date, data_interval_start, data_interval_end):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from plugins.common.clients.open_weather_client import OpenWeatherApiClient
        from plugins.common.clients.s3_client import S3Service
        from plugins.pipelines.air_pollution.extract import extract_and_store

        api_client = OpenWeatherApiClient(base_url=settings.api.url_str, api_key=settings.api.key)

        s3_hook = S3Hook(aws_conn_id="aws_default")
        boto3_client = s3_hook.get_conn()
        s3_service = S3Service(settings.aws.s3_bucket_name, s3_client=boto3_client)  # type: ignore

        s3_key_to_raw_data = extract_and_store(
            city=city_info["name"],
            lat=city_info["lat"],
            lon=city_info["lon"],
            logical_date=logical_date,
            open_weather_client=api_client,
            s3_service=s3_service,
            start_ts=data_interval_start.timestamp(),
            end_ts=data_interval_end.timestamp(),
        )

        return {"s3_key_to_raw_data": s3_key_to_raw_data, "city": city_info["name"]}

    @task
    def transform_data(extract_output: dict, logical_date):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from plugins.common.clients.s3_client import S3Service
        from plugins.pipelines.air_pollution.transform import transform_air_pollution_raw_data

        s3_key_to_raw_data = extract_output["s3_key_to_raw_data"]
        city = extract_output["city"]

        s3_hook = S3Hook(aws_conn_id="aws_default")
        boto3_client = s3_hook.get_conn()
        s3_service = S3Service(settings.aws.s3_bucket_name, s3_client=boto3_client)  # type: ignore

        air_pollution_raw_data = s3_service.load_json(s3_key_to_raw_data)
        transformed_data = transform_air_pollution_raw_data(air_pollution_raw_data, city)

        s3_key = (
            f"silver/air_pollution/"
            f"city={city}/"
            f"year={logical_date.year}/"
            f"month={logical_date.month:02d}/"
            f"day={logical_date.day:02d}/"
            f"{int(logical_date.timestamp())}.parquet"
        )
        s3_service.save_df_as_parquet(transformed_data, s3_key)

        return {"s3_key_to_transformed_data": s3_key, "city": city}

    @task
    def load_data_to_rds(transform_output: dict, logical_date):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from plugins.common.clients.postgres_loader import PostgresLoader
        from plugins.common.clients.s3_client import S3Service
        from plugins.pipelines.air_pollution.load import load_air_pollution_data

        s3_key_to_transformed_data = transform_output["s3_key_to_transformed_data"]
        city = transform_output["city"]

        s3_hook = S3Hook(aws_conn_id="aws_default")
        boto3_client = s3_hook.get_conn()
        s3_service = S3Service(settings.aws.s3_bucket_name, s3_client=boto3_client)  # type: ignore
        df = s3_service.load_parquet(s3_key_to_transformed_data)

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_dwh")
        engine = pg_hook.get_sqlalchemy_engine()

        postgres_loader = PostgresLoader(engine=engine)

        load_air_pollution_data(postgres_loader=postgres_loader, df=df, city=city)

    get_cities_config_task = get_cities_config()

    extract_tasks_group = extract_data.expand(city_info=get_cities_config_task)

    transform_tasks_group = transform_data.expand(extract_output=extract_tasks_group)

    load_data_to_rds.expand(transform_output=transform_tasks_group)

    init_table >> get_cities_config_task


air_pollution_dag()
