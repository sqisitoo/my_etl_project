import requests
import boto3
import json
import os
import yaml

from include.air_pollution.utils.s3_common import save_to_s3

API_KEY = os.getenv("AIR_POLLUTION_API_KEY")
BASE_URL = os.getenv("AIR_POLLUTION_BASE_URL")
CONFIGPATH = os.getenv("AIR_POLLUTION_CONFIGPATH")

def get_cities_config() -> list[dict]:
    with open(CONFIGPATH, 'r', encoding='UTF-8') as file:
        config = yaml.safe_load(file)
        cities_config = [{'city': city['name'], 'lat': city['lat'], 'lon': city['lon']} for city in config['cities']]

    return cities_config

def extract_air_pollution_data(*, city: str, lat: int|float|str, lon: int|float|str, start_ts: int, end_ts: int, logical_date) -> str:
    """
    Extract data from air_pollution API and load to S3.

    Args:
        city: city name,
        lat: latitude,
        lon: longitude,
        start: start of the time period (timestamp),
        end: end of the time period (timestamp),
        logical_date: DAG execution date

    Returns:
        S3 path to stored data
    """
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "start": start_ts,
        "end": end_ts
    }
    
    try:
        print("Get data from API for city {city}...")
        response = requests.get(BASE_URL, params)
        response.raise_for_status()

        data = response.json()

        if data:
            print("Got data from API!")
            s3_client = boto3.client("s3")
            
            year = logical_date.year
            month = logical_date.month
            day = logical_date.day
            
            s3_path = f'bronze/air_pollution/city={city}/year={year}/month={month:02d}/day={day:02d}/{int(logical_date.timestamp())}.json'
            json_data = json.dumps(data)

            print("Load data to s3...")

            save_to_s3(json_data, s3_path, 'application/json')

            print("Data successfully loaded to S3!")
            return s3_path

        else:
            print(f"No data for city {city} (lat={lat}, lon={lon})")

            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(f"No air pollution data available for {city}")

    except requests.exceptions.HTTPError as err:
        raise Exception(f"An HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        raise Exception(f"Another error occurred: {err}")