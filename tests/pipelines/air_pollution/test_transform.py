import pandas as pd
import pytest

from plugins.pipelines.air_pollution.transform import transform_air_pollution_raw_data


@pytest.fixture
def valid_raw_data():
    return {
        "list": [
            {
                "main": {"aqi": 1},
                "dt": 1761343200,
                "components": {
                    "co": 1.23,
                    "no": 1.10,
                    "no2": 2.24,
                    "o3": 35.95,
                    "so2": 4.47,
                    "pm2_5": 1.43,
                    "pm10": 2.43,
                    "nh3": 0.54,
                },
            },
            {
                "main": {"aqi": 5},
                "dt": 1761343300,
                "components": {
                    "co": 1.25,
                    "no": 1.05,
                    "no2": 2.21,
                    "o3": 34.89,
                    "so2": 4.14,
                    "pm2_5": 1.56,
                    "pm10": 2.76,
                    "nh3": 0.34,
                },
            },
        ]
    }


@pytest.fixture
def malformed_schema_data():
    return {
        "list": [
            {
                "main": {"aqi": 2},
                "dt": 1761343200,
                "components": {"co": 112.17, "no": 0, "no2": 4.67, "o3": 70.52},
            }
        ]
    }


def test_transform_success_structure(valid_raw_data):
    """
    Tests that the function returns a DataFrame with correct structure:
    2 rows, 14 columns, including 'city' column set to the input city.
    """
    city_name = "Berlin"
    df = transform_air_pollution_raw_data(valid_raw_data, city_name)
    assert isinstance(df, pd.DataFrame)
    assert len(df.index) == 2
    assert len(df.columns) == 14
    assert "city" in df.columns
    assert all(df["city"] == city_name)


def test_transform_value_correctness(valid_raw_data):
    """
    Tests that all transformed values (AQI, pollutants, timestamps, derived fields)
    match the expected output for the given input data.
    """
    city_name = "Berlin"
    df = transform_air_pollution_raw_data(valid_raw_data, city_name)
    # First row
    expected_dt1 = pd.to_datetime(1761343200, unit="s", utc=True)
    assert df.iloc[0]["aqi"] == 1
    assert df.iloc[0]["aqi_interpretation"] == "good"
    assert df.iloc[0]["measured_at"] == expected_dt1
    assert df.iloc[0]["day_of_week"] == expected_dt1.day_name()
    assert df.iloc[0]["time_of_day"] == expected_dt1.strftime("%H:%M")
    assert df.iloc[0]["no"] == 1.10
    assert df.iloc[0]["no2"] == 2.24
    assert df.iloc[0]["o3"] == 35.95
    assert df.iloc[0]["so2"] == 4.47
    assert df.iloc[0]["pm2_5"] == 1.43
    assert df.iloc[0]["pm10"] == 2.43
    assert df.iloc[0]["nh3"] == 0.54
    assert df.iloc[0]["co"] == 1.23
    # Second row
    expected_dt2 = pd.to_datetime(1761343300, unit="s", utc=True)
    assert df.iloc[1]["aqi"] == 5
    assert df.iloc[1]["aqi_interpretation"] == "very poor"
    assert df.iloc[1]["measured_at"] == expected_dt2
    assert df.iloc[1]["day_of_week"] == expected_dt2.day_name()
    assert df.iloc[1]["time_of_day"] == expected_dt2.strftime("%H:%M")
    assert df.iloc[1]["no"] == 1.05
    assert df.iloc[1]["no2"] == 2.21
    assert df.iloc[1]["o3"] == 34.89
    assert df.iloc[1]["so2"] == 4.14
    assert df.iloc[1]["pm2_5"] == 1.56
    assert df.iloc[1]["pm10"] == 2.76
    assert df.iloc[1]["nh3"] == 0.34
    assert df.iloc[1]["co"] == 1.25


def test_transform_empty_list():
    """Tests that ValueError is raised when the input data has an empty 'list'."""
    empty_data = {"list": []}
    with pytest.raises(ValueError):
        transform_air_pollution_raw_data(empty_data, "Berlin")


def test_transform_missing_list_key():
    """Tests that ValueError is raised when the input data is missing the 'list' key."""
    no_list_data = {"foo": []}
    with pytest.raises(ValueError):
        transform_air_pollution_raw_data(no_list_data, "Berlin")


def test_transform_malformed_schema(malformed_schema_data):
    """Tests that KeyError is raised when required columns are missing from the input data."""
    with pytest.raises(KeyError):
        transform_air_pollution_raw_data(malformed_schema_data, "Berlin")


def test_transform_aqi_categorical_integrity(valid_raw_data):
    """Tests that 'aqi_interpretation' and 'day_of_week' columns are properly set as categorical types."""
    df = transform_air_pollution_raw_data(valid_raw_data, "Berlin")

    assert isinstance(df["aqi_interpretation"].dtype, pd.CategoricalDtype)
    assert isinstance(df["day_of_week"].dtype, pd.CategoricalDtype)
