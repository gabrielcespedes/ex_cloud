from google.cloud import bigquery
import pandas as pd

def extract_from_bigquery(query: str) -> pd.DataFrame:
    client = bigquery.Client()
    return client.query(query).to_dataframe()