from pathlib import Path
from pydantic import BaseModel
from plugins.common.utils import load_yaml
from typing import Optional

class City(BaseModel):
    """
    Represents a geographic location with coordinates.
    
    Attributes:
        name (str): The name of the city.
        lat (float): Latitude coordinate of the city.
        lon (float): Longitude coordinate of the city.
    """
    name: str
    lat: float
    lon: float

class CitiesConfig(BaseModel):
    """
    Container for a collection of City objects loaded from configuration.
    
    Attributes:
        cities (list[City]): List of City objects parsed from the configuration file.
    """
    cities: list[City]


def get_cities_config(config_path: Optional[Path] = None) -> CitiesConfig:
    """
    Loads and parses the cities configuration file.
    
    This function reads a YAML configuration file containing city data,
    validates it against the CitiesConfig schema, and returns a structured object.
    If no config path is provided, it defaults to cities_config.yml in the same directory.

    Args:
        config_path (Optional[Path]): Path to the YAML configuration file. If None,
            defaults to cities_config.yml in the same directory as this module.

    Returns:
        CitiesConfig: A validated configuration object containing all configured cities.
    
    Raises:
        FileNotFoundError: If the specified configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
        pydantic.ValidationError: If the configuration data doesn't match the CitiesConfig schema.
    """
    # Use default config path if none provided
    if config_path is None:
        config_path = Path(__file__).parent / "cities_config.yml"

    # Load and parse the raw YAML data
    raw_data = load_yaml(config_path)

    # Validate and structure the data into CitiesConfig object
    return CitiesConfig(**raw_data)