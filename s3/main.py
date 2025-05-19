import config
from extract import extract_from_s3
from transform import add_discount_column
from load import load_to_s3

# ETL
df, s3_client = extract_from_s3()
df_transformed = add_discount_column(df)
load_to_s3(df_transformed, s3_client)

print("Proceso ETL en Amazon S3 finalizado con éxito.")