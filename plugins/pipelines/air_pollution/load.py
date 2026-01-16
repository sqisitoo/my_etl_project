import boto3
import os
from datetime import datetime
import json
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine, text

from plugins.common.clients.s3_common import load_parquet_from_s3

DB_USER= os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_NAME=os.getenv("DB_NAME")

def load_to_rds(path_to_transformed_data: str, city: str):
    df = load_parquet_from_s3(path_to_transformed_data)

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    with engine.begin() as connection:

        min_date = df['date'].min()
        max_date = df['date'].max()

        delete_query = text(f"""
            DELETE FROM air_pollution
            WHERE date >= :min_date 
            AND date <= :max_date
            AND city = :city
        """)

        connection.execute(delete_query, {'city': city, 'min_date': min_date, 'max_date': max_date})

        df.to_sql(name='air_pollution', con=connection, if_exists='append', index=False)
    


