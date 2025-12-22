import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
import os

BASE_DIR = "/opt/airflow/dags/datos"

DATOS_RAW_INCREMENTAL = f"{BASE_DIR}/01_Ingesta_Bronze/precios"
DATOS_RAW_FULL = f"{BASE_DIR}/01_Ingesta_Bronze/reporte_24hr"

SILVER_INCREMENTAL = f"{BASE_DIR}/02_Limpieza_Silver/precios"
SILVER_FULL = f"{BASE_DIR}/02_Limpieza_Silver/reporte_24hr"


def procesar_datos_full():

    print("Iniciando procesamiento de datos incremental")

    #Agrupo las columnas que deben ser enteros
    columnas_int = ['firstId', 'lastId', 'count']

    try:
        dt = DeltaTable(DATOS_RAW_FULL)
        df = dt.to_pandas()

        #Me quedo únicamente con los pares XXUSDT, volumen mayor a 10.000 y precios válidos
        df = df[df['symbol'].str.endswith('USDT')].copy()
        df = df[df['volume'].astype(float) > 10000].copy()
        df = df[(df['bidPrice'].astype(float) > 0) & (df['askPrice'].astype(float) > 0) & (df['priceChangePercent'].astype(float) != 0)].copy()
        
        #Formateo las columnas al tipo fecha y ajusto tipos de datos
        df[['closeTime', 'openTime']] = df[['closeTime', 'openTime']].apply(pd.to_datetime, unit='ms')
        
        #Redondeo la columna numérica a 3 decimales
        df['lastQty'] = df['lastQty'].astype(float).round(3)

        #Convierto las columnas enteras a string para evitar notación científica
        for col in columnas_int:
            if col in df.columns:
                 df[columnas_int] = df[columnas_int].astype('int64').astype(str)
        
        #Guardar en Silver
        os.makedirs(os.path.dirname(SILVER_FULL), exist_ok=True) 

        table = pa.Table.from_pandas(df)
        
        write_deltalake(
            SILVER_FULL,
            table,
            mode="overwrite"
        )
        
        return df
    
    except Exception as e:
        print(f"Ocurrió un error: {e}") 
        return None

def procesar_datos_inc():

    print("Iniciando procesamiento de datos incremental")

    try:
        dt = DeltaTable(DATOS_RAW_INCREMENTAL)
        df = dt.to_pandas()

        # Me quedo únicamente con los pares XXUSDT y precio mayor a 0
        df = df[df['symbol'].str.endswith('USDT')].copy()
        df = df[df['price'].astype(float) > 0].copy()

        os.makedirs(os.path.dirname(SILVER_INCREMENTAL), exist_ok=True)

        table = pa.Table.from_pandas(df)

        write_deltalake(
            SILVER_INCREMENTAL,
            table,
            mode="append"
        )

        return df
    
    except Exception as e:
        print(f"Ocurrió un error: {e}") 
        return None



