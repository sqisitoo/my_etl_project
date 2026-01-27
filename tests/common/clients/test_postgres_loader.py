import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from plugins.common.clients.postgres_loader import PostgresLoader

@pytest.fixture
def mock_engine_chain():
    """
    Fixture that creates a chain of mocks to simulate the following DB connection structure:
    SQLAlchemy Engine -> SQLAlchemy Connection -> psycopg2 Raw Connection -> psycopg2 Cursor.
    This allows us to test DB logic without a real database.
    Returns:
        tuple: (mock_engine, mock_sa_conn, mock_cursor)
    """
    mock_engine = MagicMock()
    # Simulate SQLAlchemy's context manager for engine.begin()
    mock_sa_conn = mock_engine.begin.return_value.__enter__.return_value
    # Simulate .connection property to get raw DBAPI connection
    mock_raw_conn = mock_sa_conn.connection
    # Simulate context manager for raw_conn.cursor()
    mock_cursor = mock_raw_conn.cursor.return_value.__enter__.return_value
    return mock_engine, mock_sa_conn, mock_cursor


def test_load_df_skips_empty_dataframe(mock_engine_chain):
    """
    If the DataFrame is empty, the loader should not attempt to open a DB transaction.
    This prevents unnecessary DB operations and errors on empty loads.
    """
    mock_engine, _, _ = mock_engine_chain
    loader = PostgresLoader(mock_engine)
    empty_df = pd.DataFrame()

    loader.load_df(empty_df, "my_table")

    mock_engine.begin.assert_not_called()


@patch("plugins.common.clients.postgres_loader.sql")
def test_load_df_executes_cleanup_query(mock_sql_module, mock_engine_chain):
    """
    If a cleanup_query is provided, it should be executed before loading the DataFrame.
    This is useful for removing or updating stale data before bulk insert.
    """
    mock_engine, mock_sa_conn, _ = mock_engine_chain
    loader = PostgresLoader(mock_engine)
    cleanup_sql = "DELETE FROM my_table WHERE date < '2026-01-01'"
    df = pd.DataFrame({'col1': [1, 2, 3]})

    loader.load_df(df, "my_table", cleanup_query=cleanup_sql)

    assert mock_sa_conn.execute.called
    args = mock_sa_conn.execute.call_args.args
    assert str(args[0]) == cleanup_sql


@patch("plugins.common.clients.postgres_loader.sql")
def test_load_df_calls_copy_expert(mock_sql_module, mock_engine_chain):
    """
    The loader should use cursor.copy_expert to efficiently bulk load the DataFrame into Postgres.
    The SQL and CSV buffer are checked for correctness.
    """
    mock_engine, _, mock_cursor = mock_engine_chain
    loader = PostgresLoader(mock_engine)
    expected_sql = "COPY pulic.my_table (...) FROM STDIN"
    df = pd.DataFrame({'col1': [1, 3], 'col2': [2, 4]})

    # Mock the SQL statement returned by the sql module
    mock_statement = mock_sql_module.SQL.return_value.format.return_value
    mock_statement.as_string.return_value = expected_sql

    loader.load_df(df, "my_table")

    assert mock_cursor.copy_expert.called

    call_args = mock_cursor.copy_expert.call_args.args
    actual_sql = call_args[0]
    csv_buffer = call_args[1]

    assert expected_sql == actual_sql
    
    content = csv_buffer.getvalue()
    # The CSV buffer should contain the data but not the header
    assert "1,2" in content
    assert "col1" not in content


@patch("plugins.common.clients.postgres_loader.sql")
def test_load_df_propagetes_exception(mock_sql_module, mock_engine_chain):
    """
    If the DB cursor raises an exception during copy_expert, it should propagate up to the caller.
    This ensures that loader errors are not silently swallowed.
    """
    mock_engine, _, mock_cursor = mock_engine_chain
    loader = PostgresLoader(mock_engine)

    mock_cursor.copy_expert.side_effect = RuntimeError("DB Crash")

    with pytest.raises(RuntimeError):
        loader.load_df(pd.DataFrame({"col1": [1]}), "my_table")







    