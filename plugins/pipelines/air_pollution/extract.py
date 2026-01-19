import logging
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from plugins.common.clients.open_weather_client import OpenWeatherApiClient
from plugins.common.clients.s3_client import S3Service

logger = logging.getLogger(__name__)

def extract_and_store(*, city: str, open_weather_client: OpenWeatherApiClient, s3_service: S3Service, lat: float, lon: float, start_ts: int|float, end_ts: int|float, logical_date: datetime) -> str:
    """
    Extracts historical air pollution data for a specified city and stores it in S3.

    Args:
        city (str): The name of the city for which to extract air pollution data.
        open_weather_client (OpenWeatherApiClient): An instance of OpenWeatherApiClient to fetch data from the OpenWeather API.
        s3_service (S3Service): An instance of S3Service to handle saving data to S3.
        lat (float): The latitude of the city.
        lon (float): The longitude of the city.
        start_ts (int|float): The start timestamp for the data extraction (in seconds since epoch).
        end_ts (int|float): The end timestamp for the data extraction (in seconds since epoch).
        logical_date (datetime): The logical date for which the data is being extracted.

    Raises:
        AirflowSkipException: If no data is found for the specified latitude and longitude.

    Returns:
        str: The S3 key where the extracted data is stored.
    """

    # Fetch historical air pollution data from the OpenWeather API
    data = open_weather_client.get_historical_airpollution_data(city=city, lat=lat, lon=lon, start_ts=start_ts, end_ts=end_ts)

    # Validate that the API response contains data
    if not data.get('list'):
        logger.warning(f"No data found for lat:{lat}, lon:{lon}")
        # Skip the task if the API returns an empty result
        raise AirflowSkipException(f"API retutned empty list for lat:{lat}, lon:{lon}")
    # Path generation (Naming convention)
    s3_key = (
        f"bronze/air_pollution/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    # Saving
    s3_service.save_dict_as_json(data, s3_key)
    
    # Return the S3 path for use in subsequent pipeline stages
    return s3_key