import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from airflow.exceptions import AirflowFailException, AirflowSkipException
from pydantic import ValidationError

from plugins.common.clients.open_weather_client import OpenWeatherApiClient
from plugins.common.clients.s3_client import S3Service
from plugins.pipelines.air_pollution.schemas import AirPollutionRecord

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Validation output including valid data, quarantine payload, and DQ status."""

    valid_records: list[dict[str, Any]]
    quarantine_records: list[dict[str, Any]]
    ts_validation: str
    is_critical_failure: bool
    failure_reason: str

def validate_data_batch(
    raw_records: list[dict[str, Any]],
    threshold_percent: float,
    min_failed_items: int,
) -> ValidationResult:
    """
    Validate a batch of air pollution records against the data schema.

    Performs Pydantic validation on raw API records and evaluates data quality thresholds.
    The function returns both valid records and quarantine payloads so downstream logic can
    persist the appropriate artifacts and decide whether to stop the pipeline.

    Args:
        raw_records (list[dict[str, Any]]): Raw air pollution records from the API.
        threshold_percent (float): Maximum acceptable failure rate as percentage.
        min_failed_items (int): Minimum number of failed items to trigger failure.

    Returns:
        ValidationResult: Valid records, quarantine records, and DQ evaluation results.
    """
    logger.debug(f"Starting validation for batch of {len(raw_records)} records")

    valid_records = []
    quarantine_records = []
    current_timestamp_isoformat = datetime.now(timezone.utc).isoformat()

    failed_count = 0
    total_count = len(raw_records)

    # Handle empty input batch
    if total_count == 0:
        logger.warning("Received empty batch for validation")
        return ValidationResult([], [], current_timestamp_isoformat, False, "")

    # Validate each record individually
    for record in raw_records:
        try:
            validated_model = AirPollutionRecord.model_validate(record)
            valid_records.append(validated_model.model_dump(mode="json"))
        except ValidationError as err:
            failed_count += 1
            logger.debug(f"Validation failed for record."
                         f"Errors: {err.errors(include_url=False)}"
                         f"Invalid record payload: {record}")
            quarantine_records.append({
                "error": err.errors(include_url=False),
                "raw": record,
                "ts": current_timestamp_isoformat
            })

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

    is_critical = (is_failure_rate_high and is_absolute_count_high) or is_total_failure

    reason = ""
    if is_critical:
        reason = (f"Threshold exceeded: {failure_rate:.2f}% failures "
                  f"(Threshold: {threshold_percent}%, MinItems: {min_failed_items})")

    return ValidationResult(
        valid_records=valid_records,
        quarantine_records=quarantine_records,
        ts_validation=current_timestamp_isoformat,
        is_critical_failure=is_critical,
        failure_reason=reason
    )




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
    dq_min_failed_items: int = 5,
) -> str:
    """
    Extract historical air pollution data from OpenWeather API and store in S3 bronze layer.

    This function orchestrates the extraction phase of the ETL pipeline by fetching raw
    air pollution data for a specific city and time range, validating the data quality,
    and persisting it to S3 in a partitioned structure. When validation finds invalid
    records, they are written to a quarantine location for later inspection.

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
        raise AirflowSkipException(f"API returned empty list for lat:{lat}, lon:{lon}")

    logger.info(f"Retrieved {len(raw_list)} raw records from API")

    # Step 3: Validate data quality and apply quality gates with injected DQ parameters
    validation_result = validate_data_batch(raw_list, dq_threshold_percent, dq_min_failed_items)

    logger.info(
        "Validation outcome: valid=%s, quarantined=%s, critical=%s",
        len(validation_result.valid_records),
        len(validation_result.quarantine_records),
        validation_result.is_critical_failure,
    )

    # Step 4: Build a partitioned base path for both valid and quarantine payloads
    base_path = (
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )
    s3_key_quarantine = f"bronze/air_pollution_quarantine/{base_path}"

    if validation_result.quarantine_records:
        logger.info("Writing %s quarantined records", len(validation_result.quarantine_records))
        quarantine_payload = {
            "metadata": {
                "status": "critical_failure" if validation_result.is_critical_failure
                                             else "partial_faiilure",
                "failure_reason": validation_result.failure_reason,
                "processed_at": validation_result.ts_validation
            },
            "records": validation_result.quarantine_records
        }

        s3_service.save_dict_as_json(quarantine_payload, s3_key_quarantine)

    else:
        logger.info("No quarantined records; removing prior quarantine artifact if it exists")
        s3_service.delete_object(s3_key_quarantine)

    if validation_result.is_critical_failure:
        logger.critical(f"Pipeline stopped due to data quality issues: {validation_result.failure_reason}")
        raise AirflowFailException(validation_result.failure_reason)

    # Step 5: Persist validated data to the bronze layer
    s3_key_valid = f"bronze/air_pollution/{base_path}"

    valid_payload = {
        "coord": data.get("coord"),
        "list": validation_result.valid_records,
        "metadata": {"status": "valid"}
    }

    s3_service.save_dict_as_json(valid_payload, s3_key_valid)
    logger.info(f"Successfully saved valid data to S3: {s3_key_valid}")
    return s3_key_valid


