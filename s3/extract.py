import boto3
import pandas as pd
from io import StringIO
import config

def extract_from_s3() -> tuple[pd.DataFrame, boto3.client]:
    s3 = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION
    )

    obj = s3.get_object(Bucket=config.S3_BUCKET, Key=config.S3_SOURCE_KEY)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))

    return df, s3