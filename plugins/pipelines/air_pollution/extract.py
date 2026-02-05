import logging
from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowFailException, AirflowSkipException
from pydantic import ValidationError

from plugins.common.clients.open_weather_client import OpenWeatherApiClient
from plugins.common.clients.s3_client import S3Service
from plugins.pipelines.air_pollution.schemas import AirPollutionRecord

logger = logging.getLogger(__name__)

def validate_data_batch(
        raw_records: list[dict[str, Any]],
        threshold_percent: float,
        min_failed_items: int
) -> list[dict[str, Any]]:
    """
    Validate a batch of air pollution records against the data schema.

    Performs Pydantic validation on raw API records and implements a quality gate
    to prevent processing critically flawed datasets. The function will fail the task
    if validation failures exceed both the percentage threshold AND absolute count,
    or if all records fail validation.

    Args:
        raw_records (list[dict[str, Any]]): Raw air pollution records from the API.
        threshold_percent (float): Maximum acceptable failure rate as percentage.
        min_failed_items (int): Minimum number of failed items to trigger failure.

    Returns:
        list[dict[str, Any]]: List of validated records that passed schema validation.

    Raises:
        AirflowFailException: If validation failures exceed quality thresholds or all records fail.
    """
    logger.debug(f"Starting validation for batch of {len(raw_records)} records")

    valid_records = []
    failed_count = 0
    total_count = len(raw_records)

    # Handle empty input batch
    if total_count == 0:
        logger.warning("Received empty batch for validation")
        return []

    # Validate each record individually
    for record in raw_records:
        try:
            validated_model = AirPollutionRecord.model_validate(record)
            valid_records.append(validated_model.model_dump(mode="json"))
        except ValidationError as err:
            failed_count += 1
            logger.error(f"Validation failed for record: {err}")

    # Calculate validation metrics
    failure_rate = (failed_count / total_count) * 100

    logger.info(
        f"Validation stats: Total={total_count}, Valid={len(valid_records)}, "
        f"Failed={failed_count} ({failure_rate:.2f}%)"
    )

    # Apply data quality gate checks
    is_failure_rate_high = failure_rate > threshold_percent
    is_absolute_count_high = failed_count >= min_failed_items

    # Edge case: Total failure indicates critical data quality issue
    is_total_failure = (len(valid_records) == 0)

    # Fail the task if quality thresholds are breached
    if (is_failure_rate_high and is_absolute_count_high) or is_total_failure:
        error_msg = (
            f"Data Quality Critical Failure: {failed_count}/{total_count} records failed "
            f"({failure_rate:.2f}%). Threshold={threshold_percent}%, MinItems={min_failed_items}."
        )
        logger.critical(error_msg)
        raise AirflowFailException(error_msg)

    logger.debug(f"Validation completed successfully with {len(valid_records)} valid records")
    return valid_records


def extract_and_store(
    *,
    city: str,
    open_weather_client: OpenWeatherApiClient,
    s3_service: S3Service,
    lat: float,
    lon: float,
    start_ts: int | float,
    end_ts: int | float,
    logical_date: datetime,
    dq_threshold_percent: float = 20.0,
    dq_min_failed_items: int = 5
) -> str:
    """
    Extract historical air pollution data from OpenWeather API and store in S3 bronze layer.

    This function orchestrates the extraction phase of the ETL pipeline by fetching raw
    air pollution data for a specific city and time range, validating the data quality,
    and persisting it to S3 in a partitioned structure. The function implements data
    quality gates and will skip the task if no valid data is available.

    Dependency Injection: Data quality parameters are configurable to support flexible
    quality thresholds across different deployment environments and use cases.

    Args:
        city (str): Name of the city for air pollution data extraction.
        open_weather_client (OpenWeatherApiClient): Client instance for OpenWeather API interaction.
        s3_service (S3Service): S3 service instance for data persistence operations.
        lat (float): Latitude coordinate of the city location.
        lon (float): Longitude coordinate of the city location.
        start_ts (int | float): Start timestamp for historical data range (Unix epoch seconds).
        end_ts (int | float): End timestamp for historical data range (Unix epoch seconds).
        logical_date (datetime): Airflow execution date used for S3 partitioning.
        dq_threshold_percent (float): Maximum acceptable failure rate as percentage. Default: 20.0.
        dq_min_failed_items (int): Minimum number of failed items to trigger failure. Default: 5.

    Returns:
        str: S3 key path where the validated data has been stored.

    Raises:
        AirflowSkipException: If API returns empty response or no records pass validation.
        AirflowFailException: If data quality thresholds are breached during validation.
    """
    logger.info(f"Starting data extraction for city='{city}' (lat={lat}, lon={lon})")
    logger.debug(f"Time range: start_ts={start_ts}, end_ts={end_ts}")
    logger.debug(
        f"Data quality parameters: threshold_percent={dq_threshold_percent}%, "
        f"min_failed_items={dq_min_failed_items}"
    )

    # Step 1: Fetch historical air pollution data from the OpenWeather API
    data = open_weather_client.get_historical_airpollution_data(
        city=city, lat=lat, lon=lon, start_ts=start_ts, end_ts=end_ts
    )

    raw_list = data.get("list")

    # Step 2: Verify API response contains data records
    if not raw_list:
        logger.warning(f"API returned empty result for lat:{lat}, lon:{lon}")
        raise AirflowSkipException(f"API retutned empty list for lat:{lat}, lon:{lon}")

    logger.info(f"Retrieved {len(raw_list)} raw records from API")

    # Step 3: Validate data quality and apply quality gates with injected DQ parameters
    valid_data_list = validate_data_batch(raw_list, dq_threshold_percent, dq_min_failed_items)

    # Handle edge case where all records fail validation
    if not valid_data_list:
        logger.warning("No valid records after validation - skipping task")
        raise AirflowSkipException("No valid records to save.")

    # Step 4: Construct final payload with coordinates and validated records
    final_payload = {
        "coord": data.get("coord"),
        "list": valid_data_list
    }

    logger.debug(f"Final payload contains {len(valid_data_list)} validated records")

    # Step 5: Generate S3 key using partitioning strategy (city/year/month/day)
    # This partitioning scheme enables efficient querying and data organization
    s3_key = (
        f"bronze/air_pollution/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    logger.info(f"Generated S3 key: {s3_key}")

    # Step 6: Persist validated data to S3 bronze layer
    s3_service.save_dict_as_json(final_payload, s3_key)

    logger.info(f"Successfully saved data to S3: {s3_key}")

    # Return S3 path for downstream pipeline stages (transform, load)
    return s3_key
