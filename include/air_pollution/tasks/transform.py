import boto3
import os
from datetime import datetime
import json
import pandas as pd
import numpy as np
import pyarrow
import io

BUCKET_NAME =  os.getenv('AWS_S3_BUCKET_NAME')