import config  # importa y ejecuta configuración de credenciales

from extract import extract_from_bigquery
from transform import add_discount_column
from load import load_to_bigquery

# Consulta a ejecutar
query = """
SELECT * FROM `spheric-gecko-435120-c8.demo_datos.ventas`
"""

# Parámetros de carga
project_id = "spheric-gecko-435120-c8"
table_id = "demo_datos.ventas_con_descuento"

# Proceso ETL
df = extract_from_bigquery(query)
df_transformed = add_discount_column(df)
load_to_bigquery(df_transformed, table_id, project_id)

print("Proceso ETL finalizado correctamente.")