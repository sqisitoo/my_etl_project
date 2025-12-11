import boto3
import os
from datetime import datetime
import json
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine

from include.air_pollution.utils.s3_common import load_parquet_from_s3

DB_USER= os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_NAME=os.getenv("DB_NAME")

def load_to_rds(path_to_transformed_data: str):
    df = load_parquet_from_s3(path_to_transformed_data)

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    df.to_sql(con=engine, name='air_pollution', if_exists='append')
    


