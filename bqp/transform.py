import pandas as pd

def add_discount_column(df: pd.DataFrame) -> pd.DataFrame:
    if 'Sales' in df.columns:
        df['Discount'] = df['Sales'] * 0.01
    elif 'total_sales' in df.columns:
        df['Discount'] = df['total_sales'] * 0.01
    else:
        raise KeyError("No se encontró la columna 'Sales' ni 'total_sales'")
    return df