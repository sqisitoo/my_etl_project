from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowSkipException

from plugins.pipelines.air_pollution.extract import extract_and_store


@pytest.fixture
def mock_clients():
    """
    Fixture that returns a tuple of MagicMock objects to mock the OpenWeather client and S3 service.
    """
    return MagicMock(), MagicMock()


def test_extract_and_store_success_path(mock_clients):
    """
    Test that extract_and_store saves data to S3 and returns the correct key when data is present.
    """
    mock_open_weather_client, mock_s3_service = mock_clients

    # Prepare fake data and expected S3 key
    fake_data, city = {"list": {"aqi": 1}}, "Berlin"
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
        lat=1.01,
        lon=2.02,
        start_ts=120000,
        end_ts=130000,
        logical_date=logical_date,
    )

    # Assert the returned key matches the expected key
    assert result_key == expected_key

    # Assert that the S3 service was called to save the data
    mock_s3_service.save_dict_as_json.assert_called_once_with(fake_data, result_key)


def test_extract_raises_skip_exception_on_empty_data(mock_clients):
    """
    Test that extract_and_store raises AirflowSkipException
    and does not call S3 when the returned data is empty.
    """
    mock_weather, mock_s3 = mock_clients

    # Mock the OpenWeather client to return empty data
    mock_weather.get_historical_airpollution_data.return_value = {"list": []}

    logical_date = datetime(2025, 1, 1, tzinfo=timezone.utc)

    # The function should raise AirflowSkipException when data is empty
    with pytest.raises(AirflowSkipException):
        extract_and_store(
            city="Berlin",
            open_weather_client=mock_weather,
            s3_service=mock_s3,
            lat=1.01,
            lon=2.02,
            start_ts=120000,
            end_ts=130000,
            logical_date=logical_date,
        )

    # Assert that the S3 service was not called
    mock_s3.save_dict_as_json.assert_not_called()
