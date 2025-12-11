import os
import boto3
import pandas as pd
import io
import json

BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")

def load_json_from_s3(s3_key: str) -> str:
    """
    Loads .json file from S3 and converts to str

    Args:
        s3_key: S3 path to csv file.

    Returns:
        json string
        
    """

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    content_string = response['Body'].read().decode('UTF-8')
    json_string = json.loads(content_string)

    return json_string

def load_parquet_from_s3(s3_key: str) -> pd.DataFrame:
    """
    Loads .parquet file from S3 and converts to DataFrame

    Args:
        s3_key: S3 path to csv file.

    Returns:
        DataFrame
        
    """

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET_NAME,Key=s3_key)

    table = pq.read_table(io.BytesIO(response['Body'].read()))

    df = table.to_pandas()

    return df