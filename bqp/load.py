import pandas as pd
import pandas_gbq

def load_to_bigquery(df: pd.DataFrame, table_id: str, project_id: str):
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="replace"
    )