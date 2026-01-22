from datetime import datetime
import json
import pandas as pd
import numpy as np
from typing import Any
import logging

logger = logging.getLogger(__name__)

def transform_air_pollution_raw_data(raw_data: dict[str, Any], city: str) -> pd.DataFrame:
    """
    Transforms raw JSON data from OpenWeather Air Pollution API into a structured DataFrame.

    This is a pure function: it contains no I/O operations (S3/DB calls).
    It performs normalization, cleaning, type casting, and data enrichment.

    Args:
        raw_data (Dict[str, Any]): The raw JSON response dictionary containing a 'list' key.
        city (str): The name of the city to add as a column for partitioning/identification.

    Returns:
        pd.DataFrame: A processed DataFrame ready for storage (Silver layer).

    Raises:
        ValueError: If the input data is empty or malformed.
        KeyError: If expected columns are missing in the source data.
    """

    # 1. Validation (Fail Fast)
    if "list" not in raw_data or not raw_data["list"]:
        logger.info("Input data failed. 'list' key missing or empty")
        raise ValueError("Data is empty or invalid structure")

    # 2. Normalization (Flattening)
    df = pd.json_normalize(raw_data["list"])
    
    # 3. Column Selection & Renaming
    rename_map = {
        'dt': 'date',
        'main.aqi': 'aqi',
        'components.no': 'no',
        'components.no2': 'no2',
        'components.o3': 'o3',
        'components.so2': 'so2',
        'components.pm2_5': 'pm2_5',
        'components.pm10': 'pm10',
        'components.nh3': 'nh3'
    }
    
    # Schema Validation
    missing_columns = set(rename_map) - set(df.columns)
    if missing_columns:
        logger.error(f"Schema mismatch. Missing columns: {missing_columns}")
        raise KeyError(f"Missing expected columns in source data: {missing_columns}")

    # Select and rename in one go
    df = df[rename_map.keys()].rename(columns=rename_map)

    # 4. Type Casting & Feature Engineering (Vectorized operations)
    # Convert Unix timestamp to datetime object
    df["measured_at"] = pd.to_datetime(df["date"], unit="s").dt.tz_localize('UTC')

    # Map AQI numeric values to human-readable categories
    aqi_categories = {1: 'good', 2: 'fair', 3: 'moderate', 4: 'poor', 5: 'very poor'}
    df["aqi_interpretation"] = df["aqi"].map(aqi_categories).astype("category")

    # Extract temporal features using .dt accessor
    df["day_of_week"] = df["datetime"].dt.day_name().astype("category")
    df["time_of_day"] = df["datetime"].dt.strftime("%H:%M")
    
    # Add partition column
    df["city"] = city

    logger.info(f"Transformation complete for city={city}. Final shape: {df.shape}")

    return df