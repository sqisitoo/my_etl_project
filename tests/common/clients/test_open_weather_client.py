from typing import Any

import pytest
import requests
import requests_mock
from pydantic import SecretStr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from plugins.common.clients.open_weather_client import OpenWeatherApiClient


@pytest.fixture
def base_url():
    return "http://api.test.com/data"


@pytest.fixture
def api_key():
    return SecretStr("secret_api_key")


@pytest.fixture
def client(base_url, api_key):
    # Use a dedicated client per test and close its session after assertions.
    client_instance = OpenWeatherApiClient(base_url, api_key)

    yield client_instance

    client_instance.session.close()


@pytest.fixture
def mock_requests():
    # requests_mock fixture to isolate outbound HTTP calls.
    with requests_mock.Mocker() as m:
        yield m


def test_get_historical_data_success_params(client, base_url, api_key, mock_requests):
    """Happy path: verifies params and response mapping for a 200 reply."""

    mock_response = {"list": [{"dt": 228}]}

    mock_requests.get(base_url, json=mock_response, status_code=200)

    result = client.get_historical_airpollution_data(
        city="TestCity", lat=10.5, lon=20.5, start_ts=10000, end_ts=20000
    )

    assert result == mock_response

    assert mock_requests.called
    assert mock_requests.call_count == 1
    assert mock_requests.last_request.method == "GET"

    query_string = mock_requests.last_request.qs

    assert query_string["lat"] == ["10.5"]
    assert query_string["lon"] == ["20.5"]
    assert query_string["start"] == ["10000"]
    assert query_string["end"] == ["20000"]
    assert query_string["appid"] == [api_key.get_secret_value()]


def test_retry_strategy_configuration(client):
    """Retry policy uses expected backoff, methods, and status codes."""

    adapter_http = client.session.get_adapter("http://test.com")
    adapter_https = client.session.get_adapter("https://test.com")

    assert isinstance(adapter_http, HTTPAdapter)
    assert isinstance(adapter_https, HTTPAdapter)

    retry_strategy = adapter_http.max_retries

    assert isinstance(retry_strategy, Retry)
    assert retry_strategy.total == 5
    assert retry_strategy.backoff_factor == 1
    assert set(retry_strategy.status_forcelist) == {500, 502, 503, 504}
    assert retry_strategy.allowed_methods == ["GET"]


def test_client_exhausted_retries_raises_error(base_url, client, mock_requests):
    """5xx responses bubble up as HTTPError after retries are exhausted."""

    mock_requests.get(base_url, status_code=500)

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        client.get_historical_airpollution_data(
            city="TestCity", lat=10.5, lon=20.5, start_ts=10000, end_ts=20000
        )

    assert excinfo.value.response.status_code == 500


def test_network_error_raises_exception(base_url, mock_requests, client):
    """Test that network errors are properly propagated."""

    mock_requests.get(base_url, exc=requests.ConnectionError("Network failed"))

    with pytest.raises(requests.ConnectionError):
        client.get_historical_airpollution_data(
            city="TestCity", lat=10.5, lon=20.5, start_ts=10000, end_ts=20000
        )


def test_context_manager_closes_session(base_url, api_key):
    """Context manager should always close the underlying session."""
    from unittest.mock import MagicMock

    client = OpenWeatherApiClient(base_url, api_key)
    client.session = MagicMock()

    with client:
        pass

    client.session.close.assert_called_once()


def test_timestamp_conversion_to_int(base_url, api_key, mock_requests):
    """Test that float timestamps are properly converted to integers."""
    mock_response: dict[str, Any] = {"list": []}
    mock_requests.get(base_url, json=mock_response, status_code=200)

    client = OpenWeatherApiClient(base_url, api_key)

    result = client.get_historical_airpollution_data(
        city="TestCity", lat=10.5, lon=20.5, start_ts=10000.7, end_ts=20000.9
    )

    assert result == mock_response

    # Verify timestamps were converted to integers in request
    query_string = mock_requests.last_request.qs
    assert query_string["start"] == ["10000"]
    assert query_string["end"] == ["20000"]

    client.session.close()


def test_timeout_exception_raised(client, mock_requests, base_url):
    """Test that timeout exceptions are properly raised."""
    mock_requests.get(base_url, exc=requests.Timeout("Request timeout"))

    with pytest.raises(requests.Timeout):
        client.get_historical_airpollution_data(
            city="TestCity", lat=10.5, lon=20.5, start_ts=10000, end_ts=20000
        )


def test_base_url_trailing_slash_removed(api_key):
    """Test that trailing slashes are removed from base URL."""
    base_url_with_slash = "http://api.test.com/data/"

    client = OpenWeatherApiClient(base_url_with_slash, api_key)

    assert client.base_url == "http://api.test.com/data"

    client.session.close()


def test_multiple_consecutive_requests(mock_requests, client, base_url):
    """Test that multiple consecutive requests work properly with session pooling."""
    mock_response_1 = {"list": [{"dt": 1}]}
    mock_response_2 = {"list": [{"dt": 2}]}

    mock_requests.get(
        base_url,
        [{"json": mock_response_1, "status_code": 200}, {"json": mock_response_2, "status_code": 200}],
    )

    result1 = client.get_historical_airpollution_data(
        city="City1", lat=10.5, lon=20.5, start_ts=10000, end_ts=20000
    )

    result2 = client.get_historical_airpollution_data(
        city="City2", lat=30.5, lon=40.5, start_ts=30000, end_ts=40000
    )

    assert result1 == mock_response_1
    assert result2 == mock_response_2
    assert mock_requests.call_count == 2
