import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging
from pydantic import SecretStr
from typing import Any


logger = logging.getLogger(__name__)

class OpenWeatherApiClient:
    """
    Client for interacting with the OpenWeatherMap API.
    Handles session management, retries, and authentication.
    """

    def __init__(self, base_url: str, api_key: SecretStr):
        """
        Initializes the API client.

        :param base_url: The base endpoint URL for the API.
        :param api_key: API Key wrapped in SecretStr for security.
        :param timeout: Default timeout for HTTP requests in seconds.
        """

        self.base_url = base_url.rstrip('/')
        self.api_key = api_key

        self.session = requests.Session()

        # Configure automatic retries for robust network interactions
        # We retry on 5xx errors (server side issues) but not on 4xx (client errors)
        retries = Retry(total=5, backoff_factor=1, allowed_methods=["GET"], status_forcelist=[500, 502, 503, 504])

        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter=adapter)
        self.session.mount("https://", adapter=adapter)

        self.session.params = {
            "appid": api_key.get_secret_value()
        }

        logger.info("Initialize OpenWeatherApiClient and start requests session")

    def get_historical_airpollution_data(self, city: str, lat: str|int|float, lon: str|int|float, start_ts: int, end_ts: int) -> dict[str, Any]:
        """
        Fetches historical air pollution data for a specific location and time range.

        :param city: Name of the city (used for logging context only).
        :param lat: Latitude.
        :param lon: Longitude.
        :param start_ts: Start timestamp (Unix).
        :param end_ts: End timestamp (Unix).
        :return: Dictionary containing the API response.
        :raises requests.HTTPError: If the API returns a non-200 status code.
        """
                
        params = {
            "lat": lat,
            "lon": lon,
            "start": start_ts,
            "end": end_ts
        }

        try:
            logger.info(f"Fetching data for city={city}(lat={lat}, lon={lon}), range={start_ts}-{end_ts}")

            # base_url is assumed to be the full endpoint path (e.g. .../air_pollution/history)
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()

            return response.json()

        except requests.HTTPError as e:
            logger.error(f"HTTP Error from OpenWeatherMap: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            raise
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type:
            logger.error(f"An error occurred: {exc_value}")

        self.session.close()
        logger.info("Requests session closed successfully")