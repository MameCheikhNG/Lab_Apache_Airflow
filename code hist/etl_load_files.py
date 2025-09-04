import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Paramètres du DAG
DAG_DIR = '/opt/airflow/files'  # répertoire contenant les fichiers
SQL_TABLE = 'dbo.CDRNM_test'            # table de destination
MSSQL_CONN_ID = 'mssql_conn'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 0
}

dag = DAG(
    'etl_load_files_direct',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

def load_files_from_directory():
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    
    files = [f for f in os.listdir(DAG_DIR) if f.endswith('.csv')]  # ou .DAT
    if not files:
        print("Aucun fichier trouvé dans le répertoire.")
        return

    for file in files:
        file_path = os.path.join(DAG_DIR, file)
        print(f"Chargement du fichier: {file_path}")
        df = pd.read_csv(file_path)  # adapter si DAT ou autre format
        hook.insert_rows(table=SQL_TABLE, rows=df.values.tolist(), target_fields=list(df.columns))
        print(f"Fichier {file} chargé avec succès.")
        # Optionnel : déplacer le fichier chargé vers un répertoire 'Loaded'
        # os.rename(file_path, os.path.join(DAG_DIR, 'Loaded', file))

load_task = PythonOperator(
    task_id='load_files_task',
    python_callable=load_files_from_directory,
    dag=dag
)
