from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import os
import glob
import pandas as pd

FILES_DIR = '/opt/airflow/files'

def list_files():
    all_files = glob.glob(os.path.join(FILES_DIR, "subscription_*.xlsx"))
    if not all_files:
        print("Aucun fichier trouvé.")
        return
    
    df_files = pd.DataFrame({'file_path': all_files})
    
    # Stocker dans SQL Server pour le DAG suivant
    hook = MsSqlHook(mssql_conn_id='mssql_conn', schema='LOG_DB')
    hook.insert_rows(
        table='dbo.files_to_process',
        rows=df_files.to_dict(orient='records'),
        target_fields=['file_path'],
        commit_every=1000
    )
    print(f"{len(all_files)} fichiers enregistrés pour traitement.")

with DAG(
    dag_id="etl_list_files",
    start_date=datetime(2025, 9, 1, 17, 0),  # La première exécution sera le 1er sept à 17h00 start_date=datetime(2025, 1, 1),
    schedule_interval="0 17 * * *",          # Tous les jours à 17h00 schedule_interval=None,
    catchup=False,
    tags=["sqlserver", "etl"],
) as dag:

    list_files_task = PythonOperator(
        task_id='list_files_task',
        python_callable=list_files
    )
