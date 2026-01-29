import pytest
from airflow.models import DagBag
import logging

logger = logging.getLogger(__name__)
DAGS_FOLDER = "dags"

@pytest.fixture(scope="module")
def dag_bag():
    """
    Fixture to provide a DagBag instance for loading and validating DAGs.
    """
    return DagBag(dag_folder=DAGS_FOLDER)

@pytest.fixture(scope="module")
def air_pollution_dag(dag_bag):
    """
    Fixture to retrieve the air_pollution_dag from the DagBag.
    Raises ValueError if the DAG is not found.
    """
    dag_id = "air_pollution_dag"
    dag = dag_bag.dags.get(dag_id)

    if dag is None:
        raise ValueError(f"DAG with id '{dag_id}' not found in {DAGS_FOLDER}")
    
    return dag

def test_air_pollution_dag_loading_no_import_errors(dag_bag):
    """
    Test that there are no import errors when loading DAGs from the dags folder.
    """
    assert len(dag_bag.import_errors) == 0

def test_air_pollution_dag_structure_and_settings(air_pollution_dag):
    """
    Test that the air_pollution_dag has the correct schedule, catchup, and default_args settings.
    """
    assert air_pollution_dag.schedule == "@daily"
    assert air_pollution_dag.catchup is False
    assert air_pollution_dag.default_args.get("retries") == 2

def test_air_pollution_dag_tasks_exist(air_pollution_dag):
    """
    Test that all expected tasks are present in the air_pollution_dag.
    """
    tasks = air_pollution_dag.task_ids
    expected_tasks = {
        "init_schema", 
        "get_cities_config", 
        "extract_data", 
        "transform_data", 
        "load_data_to_rds"
    }
    assert set(tasks) == expected_tasks

def test_air_pollution_dag_dependecies(air_pollution_dag):
    """
    Test that the dependencies between tasks in the air_pollution_dag are set up correctly.
    The expected order is: init_schema -> get_cities_config -> extract_data -> transform_data -> load_data_to_rds
    """
    init_schema = air_pollution_dag.get_task("init_schema")
    get_cities = air_pollution_dag.get_task("get_cities_config")

    assert init_schema in get_cities.upstream_list

    extract_task = air_pollution_dag.get_task("extract_data")
    transform_task = air_pollution_dag.get_task("transform_data")
    load_task = air_pollution_dag.get_task("load_data_to_rds")

    assert get_cities in extract_task.upstream_list
    assert extract_task in transform_task.upstream_list
    assert transform_task in load_task.upstream_list

    