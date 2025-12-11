import os
import boto3
import pandas as pd
import io
import json
import pyarrow.parquet as pq


def load_json_from_s3(s3_key: str, bucket_name: str = None) -> str:
    """
    Loads .json file from S3 and converts to str

    Args:
        s3_key: S3 path to csv file,
        bucket_name: S3 bucket name.

    Returns:
        json string
        
    """

    # проверяем задан ли явно s3 bucket или наличие соответствующей переменой окружение
    # в случае отстутсвия вызываем исключение
    if not bucket_name:
        bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("Bucket name is not provided and AWS_S3_BUCKET_NAME is not set.")

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    content_string = response['Body'].read().decode('UTF-8')
    json_string = json.loads(content_string)

    return json_string

def load_parquet_from_s3(s3_key: str, bucket_name: str = None) -> pd.DataFrame:
    """
    Loads .parquet file from S3 and converts to DataFrame

    Args:
        s3_key: S3 path to csv file.
        bucket_name: S3 bucket name.

    Returns:
        DataFrame
        
    """
    # проверяем задан ли явно s3 bucket или наличие соответствующей переменой окружение
    # в случае отстутсвия вызываем исключение
    if not bucket_name:
        bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("Bucket name is not provided and AWS_S3_BUCKET_NAME is not set.")

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name,Key=s3_key)

    table = pq.read_table(io.BytesIO(response['Body'].read()))

    df = table.to_pandas()

    return df

def save_to_s3(data: bytes|str, s3_key: str, content_type: str, bucket_name: str = None):
    """
    Load data to s3
    
    :param data: Description
    :type data: bytes | str
    :param s3_key: Description
    :type s3_key: str
    :param content_type: Description
    :type content_type: str
    :param bucket_name: Description
    :type bucket_name: str
    """
    
    if not bucket_name:
        bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("Bucket name is not provided and AWS_S3_BUCKET_NAME is not set.")

    if isinstance(data, str):
        data = data.encode("utf-8")

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=data,
        ContentType=content_type
    )