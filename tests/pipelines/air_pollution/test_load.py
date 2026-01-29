import pytest
import pandas as pd
from unittest.mock import MagicMock

from plugins.pipelines.air_pollution.load import load_air_pollution_data

def test_load_data_constructs_cleanup_query_correctly():
    """
    Test that load_air_pollution_data constructs the correct cleanup SQL query and parameters
    for the date range present in the DataFrame.
    """
    # Arrange: Create a mock loader and a DataFrame with a range of timestamps
    mock_loader = MagicMock()
    df = pd.DataFrame({
        "measured_at": [
            pd.Timestamp("2023-01-01 10:00:00"),
            pd.Timestamp("2023-01-01 12:00:00"),
            pd.Timestamp("2023-01-01 14:00:00")
        ],
        "aqi": [1, 2, 3]
    })
    city = "Berlin"
    
    # Act: Call the function under test
    load_air_pollution_data(mock_loader, df, city, "my_table")
    
    # Assert: Ensure the loader's load_df method was called once
    mock_loader.load_df.assert_called_once()
    
    # Extract the arguments used in the load_df call
    _, kwargs = mock_loader.load_df.call_args
    cleanup_query = kwargs["cleanup_query"]
    params = kwargs["cleanup_params"]
    
    # Assert that the SQL parameters are correct
    assert params["city"] == "Berlin"
    assert params["min_datetime"] == pd.Timestamp("2023-01-01 10:00:00")
    assert params["max_datetime"] == pd.Timestamp("2023-01-01 14:00:00")
    
    # Assert that the cleanup SQL query contains the expected structure and conditions
    assert "DELETE FROM my_table" in cleanup_query
    assert "city = :city" in cleanup_query
    assert "measured_at >= :min_datetime" in cleanup_query
    assert "measured_at <= :max_datetime" in cleanup_query

def test_load_data_validation_error():
    """
    Test that load_air_pollution_data raises a ValueError if the DataFrame is missing the 'measured_at' column,
    and ensures that the loader is not called.
    """
    mock_loader = MagicMock()
    bad_df = pd.DataFrame({"aqi": [1]})  # DataFrame missing 'measured_at'
    
    with pytest.raises(ValueError, match="must contain a 'measured_at' column"):
        load_air_pollution_data(mock_loader, bad_df, "Berlin")
        
    # Assert that the loader's load_df method was not called
    mock_loader.load_df.assert_not_called()