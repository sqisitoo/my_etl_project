import pandas as pd

from plugins.common.clients.postgres_loader import PostgresLoader


def load_air_pollution_data(
    postgres_loader: PostgresLoader, df: pd.DataFrame, city: str, table_name: str = "air_pollution"
):
    """
    Loads air pollution data into a PostgreSQL database table.

    This function deletes existing records for the specified city and datetime range
    in the target table before inserting new data from the provided DataFrame.

    Args:
        postgres_loader (PostgresLoader): An instance of PostgresLoader to interact with the database.
        df (pd.DataFrame): A DataFrame containing air pollution data to be loaded.
                           Must include a 'datetime' column.
        city (str): The name of the city for which data is being loaded.
        table_name (str): The name of the target database table. Default value is 'air_pollution'.

    Raises:
        ValueError: If the DataFrame does not contain a 'measured_at' column.
    """
    # Ensure the DataFrame contains the required 'datetime' column
    if "measured_at" not in df.columns:
        raise ValueError("The DataFrame must contain a 'measured_at' column.")

    # SQL query to delete existing records for the specified city and datetime range
    delete_query = f"""
        DELETE FROM {table_name}
        WHERE city = :city
        AND measured_at >= :min_datetime
        AND measured_at <= :max_datetime
    """

    # Parameters for the delete query, derived from the DataFrame and city argument
    params = {
        "city": city,
        "min_datetime": df["measured_at"].min(),  # Minimum datetime in the DataFrame
        "max_datetime": df["measured_at"].max(),  # Maximum datetime in the DataFrame
    }

    # Load the DataFrame into the database table, with cleanup using the delete query
    postgres_loader.load_df(df, table_name=table_name, cleanup_query=delete_query, cleanup_params=params)
