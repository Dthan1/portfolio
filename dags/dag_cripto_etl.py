from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from extraccion import extraer_datos_full, extraer_datos_inc, guardar_datos_full, guardar_datos_inc
from transformacion import procesar_datos_full, procesar_datos_inc
from cargar_SnowFlake import cargar_snowflake

#Definir argumentos por defecto
default_args = {
    'owner': 'tu_nombre',
    'retries': 1,              # Si falla, reintenta 1 vez
    'retry_delay': timedelta(minutes=5), # Espera 5 min antes de reintentar
}

#Definir funciones wrap para las tareas ETL
#Airflow necesita funciones que engloben la lógica de cada paso.

def reporte_diario():
    print("Iniciando ETL Reporte Diario")
    
    datos_full = extraer_datos_full()
    if not datos_full:
        raise ValueError("No se extrajeron datos del reporte")
    elif datos_full:
        guardar_datos_full(datos_full)
    else:
        raise ValueError("La extracción falló, no se obtuvieron datos del reporte")

    df_full = procesar_datos_full()
    if df_full is None or df_full.empty:
        raise ValueError("El DataFrame del reporte está vacío")
        

    cargar_snowflake(df_full, "REPORTE_DIARIO")
    print("ETL Reporte Diario completado")

def precios():
    print("Iniciando ETL Precios Actuales")

    datos_inc = extraer_datos_inc()
    if not datos_inc:
        raise ValueError("No se extrajeron precios actuales")
    guardar_datos_inc(datos_inc)
    

    df_inc = procesar_datos_inc()
    if df_inc is None or df_inc.empty:
        raise ValueError("El DataFrame de precios está vacío")
        


    cargar_snowflake(df_inc, "PRECIOS_ACTUALES")
    print("ETL Precios Actuales completado")


#Definir DAG
with DAG(
    dag_id='etl_binance_snowflake_v1',
    default_args=default_args,
    description='ETL de Criptomonedas a Snowflake',
    start_date=datetime(2024, 1, 1), #Fecha pasada para iniciar inmediatamente
    schedule_interval='@daily',       #Ejecutar diariamente
    catchup=False,                    #No intentar ejecutar días pasados perdidos
    tags=['cripto', 'etl']
) as dag:

    #Definición de tareas (Operators)
    
    t1_reporte = PythonOperator(
        task_id='etl_reporte_diario',
        python_callable=reporte_diario
    )

    t2_precios = PythonOperator(
        task_id='etl_precios_act',
        python_callable=precios
    )

    #Definir el orden de ejecución de las tareas
    #t1 corre primero y luego t2
    t1_reporte >> t2_precios