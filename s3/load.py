from io import StringIO
import pandas as pd
import config

def load_to_s3(df: pd.DataFrame, s3_client):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=config.S3_BUCKET,
        Key=config.S3_DEST_KEY,
        Body=csv_buffer.getvalue()
    )