from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowFailException, AirflowSkipException

from plugins.pipelines.air_pollution.extract import extract_and_store, validate_data_batch


@pytest.fixture
def valid_record_raw():
    """Returns a single valid dictionary that conforms to the Pydantic schema."""
    return {
        "dt": 1606482000,
        "main": {"aqi": 2},
        "components": {
            "co": 200.0,
            "no": 10.0,
            "no2": 10.0,
            "o3": 10.0,
            "so2": 10.0,
            "pm2_5": 10.0,
            "pm10": 10.0,
            "nh3": 10.0,
        },
    }


@pytest.fixture
def invalid_record_raw():
    """Dictionary that will fail validation due to type and schema errors."""
    return {
        "dt": 1606482000,
        "main": {"aqi": "NOT_AN_INT"},  # Type error: AQI must be an integer
        # Missing 'components' field -> Schema validation error
    }


@pytest.fixture
def mock_clients():
    """
    Fixture that returns a tuple of MagicMock objects to mock the OpenWeather client and S3 service.
    """
    return MagicMock(), MagicMock()


def test_extract_and_store_success_path(mock_clients, valid_record_raw):
    """
    Test that extract_and_store saves data to S3 and returns the correct key when data is present.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Prepare fake data and expected S3 key
    fake_data, city = {"coord": {"lon": 50.0, "lat": 50.0}, "list": [valid_record_raw] * 24}, "Berlin"
    logical_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    expected_key = (
        f"bronze/air_pollution/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    # Mock the OpenWeather client to return fake data
    mock_open_weather_client.get_historical_airpollution_data.return_value = fake_data

    # Call the function under test
    result_key = extract_and_store(
        city=city,
        open_weather_client=mock_open_weather_client,
        s3_service=mock_s3_service,
        lat=50.0,
        lon=50.0,
        start_ts=120000,
        end_ts=130000,
        logical_date=logical_date,
    )

    # Assert the returned key matches the expected key
    assert result_key == expected_key

    # Assert that the S3 service was called to save the data
    expected_dict = fake_data | {"metadata": {"status": "valid"}}
    mock_s3_service.save_dict_as_json.assert_called_once_with(expected_dict, result_key)

    # Unpack call_args: args=positional args tuple, _=kwargs (ignored)
    args, _ = mock_s3_service.save_dict_as_json.call_args
    saved_data = args[0]
    assert len(saved_data["list"]) == 24


def test_extract_and_store_particular_failure_success(mock_clients, valid_record_raw, invalid_record_raw):
    """
    Test that extract_and_store succeeds when validation failures are below quality thresholds.

    This test verifies that the function processes a mixed batch (21 valid + 3 invalid records)
    where the failure rate (12.5%) is below the threshold (20%), allowing the task to proceed
    successfully and save only valid records to S3.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Create a batch with majority valid records (87.5% pass rate)
    mixed_list = ([valid_record_raw] * 21) + ([invalid_record_raw] * 3)

    mock_open_weather_client.get_historical_airpollution_data.return_value = {
        "coord": [50.0, 50.0],
        "list": mixed_list,
    }

    # Execute the extraction with DQ parameters
    extract_and_store(
        city="Berlin",
        open_weather_client=mock_open_weather_client,
        s3_service=mock_s3_service,
        lat=50.0,
        lon=50.0,
        start_ts=120000,
        end_ts=130000,
        logical_date=datetime(2025, 1, 1),
        dq_threshold_percent=20,
        dq_min_failed_items=5,
    )

    # Verify that only valid records were saved (invalid records filtered out)
    # Extract args tuple from call_args, then [0]=first arg (data dict)
    args = mock_s3_service.save_dict_as_json.call_args.args
    saved_data = args[0]
    assert len(saved_data["list"]) == 21

    # Verify that quarantine was written (save_dict_as_json called twice)
    assert mock_s3_service.save_dict_as_json.call_count == 2


def test_extract_and_store_critical_failure(mock_clients, valid_record_raw, invalid_record_raw):
    """
    Test that extract_and_store raises AirflowFailException when data quality thresholds are breached.

    This test verifies the critical failure path where validation failures (50% failure rate)
    exceed both the percentage threshold (20%) AND the minimum failed items threshold (5),
    triggering an AirflowFailException and preventing data from being saved to S3.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Create a batch with equal valid and invalid records (50% failure rate)
    mixed_list = ([valid_record_raw] * 10) + ([invalid_record_raw] * 10)

    mock_open_weather_client.get_historical_airpollution_data.return_value = {
        "coord": [50.0, 50.0],
        "list": mixed_list,
    }

    # Expect the function to raise AirflowFailException due to critical data quality issues
    with pytest.raises(AirflowFailException):
        extract_and_store(
            city="Berlin",
            open_weather_client=mock_open_weather_client,
            s3_service=mock_s3_service,
            lat=50.0,
            lon=50.0,
            start_ts=120000,
            end_ts=130000,
            logical_date=datetime(2025, 1, 1),
            dq_threshold_percent=20,
            dq_min_failed_items=5,
        )

    # Verify that valid data was NOT saved to S3
    valid_calls = [
        c
        for c in mock_s3_service.save_dict_as_json.call_args_list
        if "air_pollution/" in str(c) and "quarantine" not in str(c)
    ]
    assert len(valid_calls) == 0


def test_extract_and_store_raises_skip_exception_on_empty_data(mock_clients):
    """
    Test that extract_and_store raises AirflowSkipException
    and does not call S3 when the returned data is empty.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Mock the OpenWeather client to return empty data
    mock_open_weather_client.get_historical_airpollution_data.return_value = {"list": []}

    logical_date = datetime(2025, 1, 1, tzinfo=timezone.utc)

    # The function should raise AirflowSkipException when data is empty
    with pytest.raises(AirflowSkipException):
        extract_and_store(
            city="Berlin",
            open_weather_client=mock_open_weather_client,
            s3_service=mock_s3_service,
            lat=1.01,
            lon=2.02,
            start_ts=120000,
            end_ts=130000,
            logical_date=logical_date,
        )

    # Assert that the S3 service was not called
    mock_s3_service.save_dict_as_json.assert_not_called()


def test_validate_data_batch_returns_validation_result(valid_record_raw, invalid_record_raw):
    """
    Test that validate_data_batch returns a ValidationResult with correct structure.

    Verifies that the function properly separates valid and invalid records,
    calculates failure rates, and determines critical failure status.
    """
    mixed_list = ([valid_record_raw] * 18) + ([invalid_record_raw] * 2)

    result = validate_data_batch(mixed_list, threshold_percent=20.0, min_failed_items=5)

    # Verify result structure and values
    assert len(result.valid_records) == 18
    assert len(result.quarantine_records) == 2
    assert result.is_critical_failure is False  # 10% < 20% and 2 < 5
    assert result.failure_reason == ""
    assert result.ts_validation  # Timestamp should be populated
    assert all(
        "error" in record and "raw" in record and "ts" in record for record in result.quarantine_records
    )


def test_validate_data_batch_critical_failure_detection(valid_record_raw, invalid_record_raw):
    """
    Test that validate_data_batch correctly identifies critical failures.

    Verifies that when failures exceed BOTH percentage threshold AND min items,
    the result flags is_critical_failure=True with proper failure reason.
    """
    # 40% failure rate with 4 failed items
    mixed_list = ([valid_record_raw] * 6) + ([invalid_record_raw] * 4)

    result = validate_data_batch(mixed_list, threshold_percent=20.0, min_failed_items=5)

    # Fails percentage check (40% > 20%) but not absolute count check (4 < 5)
    assert result.is_critical_failure is False
    assert result.failure_reason == ""

    # Now test with both checks exceeded
    mixed_list = ([valid_record_raw] * 6) + ([invalid_record_raw] * 6)

    result = validate_data_batch(mixed_list, threshold_percent=20.0, min_failed_items=5)

    # Exceeds both checks (50% > 20% AND 6 >= 5)
    assert result.is_critical_failure is True
    assert "Threshold exceeded" in result.failure_reason
    assert "50.00%" in result.failure_reason


def test_extract_and_store_partial_failure_with_quarantine_write(
    mock_clients, valid_record_raw, invalid_record_raw
):
    """
    Test that extract_and_store writes quarantine records to S3 for partial failures.

    Verifies that when some records fail validation but below critical thresholds,
    the invalid records are written to the quarantine location with proper metadata.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Create a batch with partial failures (12.5% failure rate, below threshold)
    mixed_list = ([valid_record_raw] * 21) + ([invalid_record_raw] * 3)

    mock_open_weather_client.get_historical_airpollution_data.return_value = {
        "coord": {"lon": 50.0, "lat": 50.0},
        "list": mixed_list,
    }

    city = "Berlin"
    logical_date = datetime(2025, 1, 1)

    extract_and_store(
        city=city,
        open_weather_client=mock_open_weather_client,
        s3_service=mock_s3_service,
        lat=50.0,
        lon=50.0,
        start_ts=120000,
        end_ts=130000,
        logical_date=logical_date,
        dq_threshold_percent=20,
        dq_min_failed_items=5,
    )

    # Verify quarantine was written
    expected_quarantine_key = (
        f"bronze/air_pollution_quarantine/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    # Verify save_dict_as_json was called twice: once for quarantine, once for valid
    assert mock_s3_service.save_dict_as_json.call_count == 2

    # Check that quarantine payload was written with correct structure
    calls = mock_s3_service.save_dict_as_json.call_args_list
    quarantine_call = [c for c in calls if expected_quarantine_key in str(c)]
    assert len(quarantine_call) == 1

    # Extract payload from mock call: [0]=call object, [0]=args tuple, [0]=first arg (data dict)
    quarantine_payload = quarantine_call[0][0][0]
    assert "metadata" in quarantine_payload
    assert quarantine_payload["metadata"]["status"] == "partial_faiilure"
    assert "records" in quarantine_payload
    assert len(quarantine_payload["records"]) == 3


def test_extract_and_store_all_valid_records_deletes_quarantine(mock_clients, valid_record_raw):
    """
    Test that extract_and_store deletes quarantine artifact when all records are valid.

    Verifies that when validation produces zero quarantine records,
    any prior quarantine artifact is removed from S3.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # All valid records
    valid_list = [valid_record_raw] * 24

    mock_open_weather_client.get_historical_airpollution_data.return_value = {
        "coord": {"lon": 50.0, "lat": 50.0},
        "list": valid_list,
    }

    city = "Berlin"
    logical_date = datetime(2025, 1, 1)

    extract_and_store(
        city=city,
        open_weather_client=mock_open_weather_client,
        s3_service=mock_s3_service,
        lat=50.0,
        lon=50.0,
        start_ts=120000,
        end_ts=130000,
        logical_date=logical_date,
    )

    # Verify delete_object was called for quarantine
    expected_quarantine_key = (
        f"bronze/air_pollution_quarantine/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    mock_s3_service.delete_object.assert_called_once_with(expected_quarantine_key)

    # Verify valid data was still saved
    mock_s3_service.save_dict_as_json.assert_called_once()
    # Extract first arg from call_args: [0]=args tuple, [0]=first arg (data dict)
    saved_data = mock_s3_service.save_dict_as_json.call_args[0][0]
    assert len(saved_data["list"]) == 24
    assert saved_data["metadata"]["status"] == "valid"


def test_extract_and_store_critical_failure_with_quarantine_saved(
    mock_clients, valid_record_raw, invalid_record_raw
):
    """
    Test that extract_and_store saves quarantine records before raising exception on critical failure.

    Verifies that even when critical failure occurs, the quarantine artifacts are
    persisted to S3 for later inspection before the task is failed.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Create a batch with critical failure (50% failure rate)
    mixed_list = ([valid_record_raw] * 10) + ([invalid_record_raw] * 10)

    mock_open_weather_client.get_historical_airpollution_data.return_value = {
        "coord": {"lon": 50.0, "lat": 50.0},
        "list": mixed_list,
    }

    city = "Berlin"
    logical_date = datetime(2025, 1, 1)

    # Verify exception is raised
    with pytest.raises(AirflowFailException):
        extract_and_store(
            city=city,
            open_weather_client=mock_open_weather_client,
            s3_service=mock_s3_service,
            lat=50.0,
            lon=50.0,
            start_ts=120000,
            end_ts=130000,
            logical_date=logical_date,
            dq_threshold_percent=20,
            dq_min_failed_items=5,
        )

    # Verify quarantine was saved despite critical failure
    expected_quarantine_key = (
        f"bronze/air_pollution_quarantine/"
        f"city={city}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"{int(logical_date.timestamp())}.json"
    )

    calls = mock_s3_service.save_dict_as_json.call_args_list
    quarantine_call = [c for c in calls if expected_quarantine_key in str(c)]
    assert len(quarantine_call) == 1

    # Extract payload from mock call: [0]=call object, [0]=args tuple, [0]=first arg (data dict)
    quarantine_payload = quarantine_call[0][0][0]
    assert quarantine_payload["metadata"]["status"] == "critical_failure"
    assert "Threshold exceeded" in quarantine_payload["metadata"]["failure_reason"]
    assert len(quarantine_payload["records"]) == 10

    # Verify valid data was NOT saved (no second save_dict call for valid data)
    valid_calls = [c for c in calls if "air_pollution/" in str(c) and "quarantine" not in str(c)]
    assert len(valid_calls) == 0
