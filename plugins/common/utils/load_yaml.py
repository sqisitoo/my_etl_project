from pathlib import Path
from typing import Any

import yaml


def load_yaml(path_to_yaml: Path) -> Any:
    """
    Loads and parses a YAML/YML file into Python objects.

    This function safely loads a YAML file using the safe_load method,
    which prevents arbitrary Python code execution from untrusted sources.

    Args:
        path_to_yaml (Path): The file path to the YAML/YML file to load.

    Returns:
        Any: The parsed data structure (typically dict or list, but can be int, str, or None).

    Raises:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If there is an error while parsing the YAML file syntax.
    """
    try:
        # Open and safely parse the YAML file
        with open(path_to_yaml) as file:
            data = yaml.safe_load(file)
        return data
    except FileNotFoundError as e:
        # File does not exist at the specified path
        raise FileNotFoundError(f"YAML file not found: {path_to_yaml}") from e
    except yaml.YAMLError as e:
        # YAML syntax or parsing error occurred
        raise yaml.YAMLError(f"Error parsing YAML file: {path_to_yaml}") from e
