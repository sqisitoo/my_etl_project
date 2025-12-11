import boto3
import os
from datetime import datetime
import json
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine


BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")

DB_USER= os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_NAME=os.getenv("DB_NAME")

def load_to_rds(path_to_transformed_data: str):

    # s3 = boto3.client('s3')
    # response = s3.get_object(Bucket=BUCKET_NAME,Key=path_to_transformed_data)

    # table = pq.read_table(io.BytesIO(response['Body'].read()))

    # df = table.to_pandas()
    from include.air_pollution.utils.s3_common import load_parquet_from_s3
    df = load_parquet_from_s3(path_to_transformed_data)

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    df.to_sql(con=engine, name='air_pollution', if_exists='append')
    


