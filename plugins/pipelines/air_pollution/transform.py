import boto3
import os
from datetime import datetime
import json
import pandas as pd
import numpy as np
import pyarrow
import io

from plugins.common.clients.s3_common import load_json_from_s3
from plugins.common.clients.s3_common import save_to_s3

def transform_air_pollution_data(*, s3_key: str, city: str, logical_date) -> str:
    """
    Transforms raw air_pollution_data and load to s3.

    Args:
        s3_key: s3 key to stored raw data
        city: city name,
        logical_date: DAG execution date

    Returns:
        S3 key to stored transformed data
    """

    data = load_json_from_s3(s3_key)

    # extract list with essential data
    data_list = data['list']
    # create dicts list for DataFrame creating
    lst = [
            {'date': datetime.fromtimestamp(data_dict['dt']),
             'aqi': data_dict['main']['aqi'],
             'no': data_dict['components']['no'],
             'no2': data_dict['components']['no2'],
             'o3': data_dict['components']['o3'],
             'so2': data_dict['components']['so2'],
             'pm2_5': data_dict['components']['pm2_5'],
             'pm10': data_dict['components']['pm10'],
             'nh3': data_dict['components']['nh3']
            } for data_dict in data_list]

    # create base DataFrame
    base_df = pd.DataFrame(lst)

    #data encrichment
    aqi_categories = {1: 'good', 2: 'fair', 3: 'moderate', 4: 'poor', 5: 'very poor'}
    aqi_interpretation = base_df['aqi'].map(aqi_categories)
    
    day_of_week = base_df['date'].dt.day_name()
    time_of_day = base_df['date'].apply(lambda row: row.strftime('%H:%M'))

    final_df = base_df.assign(aqi_interpretation=aqi_interpretation,
                              day_of_week=day_of_week,
                              time_of_day=time_of_day,
                              city=city
                             )

    final_df = final_df.astype({
        'aqi_interpretation': 'category',
        'day_of_week': 'category'
    })

    parquet_buffer = io.BytesIO()

    final_df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

    year = logical_date.year
    month = logical_date.month
    day = logical_date.day
            
    s3_key = f'silver/air_pollution/city={city}/year={year}/month={month:02d}/day={day:02d}/{int(logical_date.timestamp())}.parquet'

    save_to_s3(parquet_buffer.getvalue(), s3_key, 'application/parquet')

    return s3_key