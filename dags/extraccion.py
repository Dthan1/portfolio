import requests
import pandas as pd
import pyarrow as pa
from datetime import datetime
from deltalake import write_deltalake
import os
import sys

URL_INCREMENTAL = "https://data-api.binance.vision/api/v3/ticker/price"
URL_FULL = "https://data-api.binance.vision/api/v3/ticker/24hr"

BASE_DIR = "/opt/airflow/dags/datos"
DATOS_RAW_INCREMENTAL = f"{BASE_DIR}/01_Ingesta_Bronze/precios"
DATOS_RAW_FULL = f"{BASE_DIR}/01_Ingesta_Bronze/reporte_24hr"

def extraer_datos_inc():
    try:
        respuesta = requests.get(URL_INCREMENTAL, timeout=10) #Timeout para que no se cuelgue
        respuesta.raise_for_status()
       
        datos = respuesta.json()
        return datos

    except Exception as e:
        print(f"Ocurrió un error: {e}") 

def extraer_datos_full():
    try:
        respuesta = requests.get(URL_FULL, timeout=10)
        respuesta.raise_for_status()

        datos = respuesta.json()
        return datos
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")

def guardar_datos_inc(datos):
    if not datos:
        print("No hay datos para guardar")
        return
    
    try:
        df = pd.DataFrame(datos)

        df['fecha_ingreso'] = datetime.now()

        table = pa.Table.from_pandas(df)

        os.makedirs(os.path.dirname(DATOS_RAW_INCREMENTAL), exist_ok=True)

        write_deltalake(
            DATOS_RAW_INCREMENTAL,
            table,
            mode="append"
        )

        print(f"Datos guardados en: {DATOS_RAW_INCREMENTAL}")
    except Exception as e:
        print(f"Ocurrió un error al guardar los datos incrementales: {e}")
        raise e
    

def guardar_datos_full(datos):
    if not datos:
        print("No hay datos para guardar")
        return
   
    try:

        df = pd.DataFrame(datos)

        df['fecha_ingreso'] = datetime.now()

        table = pa.Table.from_pandas(df)

        os.makedirs(os.path.dirname(DATOS_RAW_FULL), exist_ok=True)

        write_deltalake(
            DATOS_RAW_FULL,
            table,
            mode="overwrite",
        )

        print(f"Datos guardados en: {DATOS_RAW_FULL}")
    except Exception as e:
        print(f"Ocurrió un error al guardar los datos full: {e}")
        raise e
