import logging
from datetime import datetime

from airflow.exceptions import AirflowSkipException

from plugins.common.clients.open_weather_client import OpenWeatherApiClient
from plugins.common.clients.s3_client import S3Service

logger = logging.getLogger(__name__)


def extract_air_pollution_to_s3(
    *,
    city: str,
    open_weather_client: OpenWeatherApiClient,
    s3_service: S3Service,
    lat: float,
    lon: float,
    start_ts: int | float,
    end_ts: int | float,
    logical_date: datetime,
) -> str:
    """Fetch air pollution records for a city and persist them to S3.

    The function calls OpenWeather's historical API for the requested time
    window, validates that at least one record was returned, stores the raw
    payload as JSON in an S3 bronze path partitioned by city and logical date,
    and returns the written S3 key.

    Raises:
        AirflowSkipException: If the API response does not contain any records.
    """
    # Pull raw historical records for the requested city and time window.
    data = open_weather_client.get_historical_airpollution_data(
        city=city, lat=lat, lon=lon, start_ts=start_ts, end_ts=end_ts
    )

    raw_list = data.get("list")

    # Skip downstream work when the upstream API has no records.
    if not raw_list:
        logger.warning(f"API returned empty result for lat:{lat}, lon:{lon}")
        raise AirflowSkipException(f"API returned empty list for lat:{lat}, lon:{lon}")

    logger.info(f"Retrieved {len(raw_list)} raw records from API")

    # Build a partitioned bronze key for traceable and query-friendly storage.
    s3_key = (
        f"bronze/air_pollution/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    s3_service.save_dict_as_json(data, s3_key)
    logger.info(f"Successfully saved data to S3: {s3_key}")
    return s3_key
