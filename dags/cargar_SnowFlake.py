import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

#Cargar credenciales desde el archivo .env
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SF_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') # Valor por defecto si no existe
SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'CRIPTO_DB')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
SF_ROLE = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')

def cargar_snowflake(df, tabla):

    #Establecer conexión con Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='COMPUTE_WH',
        database='CRIPTO_DB',
        schema='BRONZE'
    )

    try:
        #Crear tabla si no existe
        cursor = conn.cursor()
        cursor.execute(f"USE WAREHOUSE COMPUTE_WH")
        cursor.execute(f"USE DATABASE CRIPTO_DB")
        cursor.execute(f"USE SCHEMA BRONZE")

        #Escribir el DataFrame en Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            tabla.upper(), #Snowflake prefiere nombres en mayúsuculas
            auto_create_table=True
        )

        print(f"Datos cargados en Snowflake: {nrows} filas insertadas en la tabla {tabla}")

    except Exception as e:
        print(f"Error al cargar datos en Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Creamos datos falsos para probar ahora mismo
    df_test = pd.DataFrame({
        'ID': [1, 2, 3],
        'MONEDA': ['Bitcoin', 'Ethereum', 'Solana'],
        'PRECIO': [50000, 3000, 150]
    })
    
    print("🚀 Iniciando prueba de carga a Snowflake...")
    # Llamamos a tu función
    cargar_snowflake(df_test, "TEST_CARGA_PYTHON")