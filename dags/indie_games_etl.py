from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os


# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 21),
    'retries': 1
}

dag = DAG(
    'INDIE_GAMES_DATA_ETL',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

CSV_PATH = os.path.join(os.path.dirname(__file__), "../files/videogamesales2024.csv") # Variável de ambiente ou caminho padrão
TABLE_NAME = 'game_sales_data'

def read_data(**kwargs):
    try:
        df = pd.read_csv(CSV_PATH)
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
        print(f"Arquivo CSV lido com sucesso: {CSV_PATH}")
    except FileNotFoundError:
        print(f"Erro: Arquivo CSV não encontrado em: {CSV_PATH}")
        raise  # Re-lança a exceção para que a task falhe
    except Exception as e:
        print(f"Erro na leitura do CSV: {e}")
        raise

def transform_data(**kwargs):
    try:
        ti = kwargs['ti']
        df = pd.read_json(ti.xcom_pull(task_ids='read_data', key='raw_data'))

        df = df.drop_duplicates().dropna()
        if 'img' in df.columns:
            df = df.drop(columns=['img'])

        ti.xcom_push(key='cleaned_data', value=df.to_json())
        print("Dados transformados com sucesso.")
    except Exception as e:
        print(f"Erro na transformação dos dados: {e}")
        raise



def load_data(**kwargs):
    try:
        ti = kwargs['ti']
        df = pd.read_json(ti.xcom_pull(task_ids='transform_data', key='cleaned_data'))        
        
        hook = PostgresHook(postgres_conn_id='postgres')
        engine = hook.get_sqlalchemy_engine()
        
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, chunksize=10000)  
        print("Dados carregados com sucesso no PostgreSQL.")

    except Exception as e:
        print(f"Erro no carregamento dos dados: {e}")
        raise


read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)



load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

read_data_task >> transform_data_task >> load_data_task