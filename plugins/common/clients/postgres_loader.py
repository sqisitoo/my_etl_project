from datetime import datetime
import pandas as pd
import io
from sqlalchemy import create_engine, text
from psycopg2 import sql
import logging
from sqlalchemy.engine import Engine
import csv
from typing import Optional, Any

logger = logging.getLogger(__name__)

class PostgresLoader:
    """
    A utility class for efficiently loading pandas DataFrames into PostgreSQL tables.
    
    This loader uses the COPY command for high-performance bulk inserts, converting
    DataFrames to CSV format before streaming them into the database.
    
    Attributes:
        _engine (Engine): SQLAlchemy engine instance for database connections.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize the PostgresLoader with a SQLAlchemy engine.
        
        Args:
            engine (Engine): SQLAlchemy engine connected to the target PostgreSQL database.
        """
        self._engine = engine

    def _prepare_buffer_from_df(self, df: pd.DataFrame) -> io.StringIO:
        """
        Convert a pandas DataFrame to a CSV-formatted buffer for streaming.
        
        Args:
            df (pd.DataFrame): The DataFrame to convert.
            
        Returns:
            io.StringIO: A buffer containing CSV data ready for COPY operations.
        """
        buffer = io.StringIO()

        # Convert DataFrame to CSV format with minimal quoting for efficiency
        df.to_csv(
            buffer,
            index=False,
            sep=',',
            header=False,
            na_rep='',
            quoting=csv.QUOTE_MINIMAL
        )

        buffer.seek(0)
        return buffer

    def load_df(self, df: pd.DataFrame, table_name: str, schema: Optional[str] = 'public', *, cleanup_query: Optional[str] = None, 
                cleanup_params: Optional[dict[str, Any]] = None):
        """
        Load a pandas DataFrame into a PostgreSQL table using the COPY command.
        
        This method executes an optional cleanup query before performing the bulk insert.
        The transaction is automatically committed if successful, or rolled back on error.
        
        Args:
            df (pd.DataFrame): The DataFrame containing data to load.
            table_name (str): The name of the target table.
            schema (str, optional): The schema containing the table. Defaults to 'public'.
            cleanup_query (str, optional): SQL query to execute before loading data. Defaults to None.
            cleanup_params (dict, optional): Parameters for the cleanup query. Defaults to None.
            
        Raises:
            Exception: If the database operation fails, the exception is logged and re-raised.
        """
        
        # Skip empty DataFrames to avoid unnecessary operations
        if df.empty:
            logger.warning(f"DataFrame for {schema}.{table_name} is empty. Skipping.")
            return

        csv_buffer = self._prepare_buffer_from_df(df)
        columns = list(df.columns)

        # Build fully-qualified table identifier with schema
        table_id = sql.SQL("{}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        
        try:
            # Use transaction context to ensure atomicity
            with self._engine.begin() as conn:
                # Execute cleanup operation if provided
                if cleanup_query:
                    conn.execute(text(cleanup_query), cleanup_params or {})
                    logger.info("Cleanup query executed.")

                # Get raw connection for cursor-based COPY operation
                raw_conn = conn.connection
                with raw_conn.cursor() as cursor:

                    # Build COPY statement with proper quoting and NULL handling
                    copy_stmt = sql.SQL("""
                        COPY {} ({}) FROM STDIN WITH(
                            FORMAT CSV,
                            NULL ''
                        )
                    """).format(
                        table_id,
                        sql.SQL(', ').join(map(sql.Identifier, columns))
                    )

                    sql_string = copy_stmt.as_string(raw_conn)

                    logger.info(f"Loading {len(df)} rows into {schema}.{table_name}")
                    # Stream CSV data directly into PostgreSQL
                    cursor.copy_expert(sql_string, csv_buffer)
            
            logger.info("Transaction committed.")
            
        except Exception as err:
            logger.error(f"Failed to load data: {err}")
            raise