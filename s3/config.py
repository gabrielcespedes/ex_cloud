import pandas as pd

# Leer claves desde archivo .csv
keys_df = pd.read_csv("ps3_accessKeys.csv")

# Acceso como variables globales
AWS_ACCESS_KEY = keys_df.loc[0, "Access key ID"]
AWS_SECRET_KEY = keys_df.loc[0, "Secret access key"]

# Región por defecto del bucket
AWS_REGION = "us-east-2"

# Nombre del bucket
S3_BUCKET = "data-ventas-cespedes"

# Nombre del archivo fuente
S3_SOURCE_KEY = "Sales_Retail.csv"

# Nombre del archivo destino (resultado)
S3_DEST_KEY = "resultados/ventas_con_descuento.csv"