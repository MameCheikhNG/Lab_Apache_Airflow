from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import os

def create_table():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    
    # Chemin complet du fichier SQL dans le conteneur
    sql_file_path = '/opt/airflow/dags/sql/create_daily_subscription.sql'
    
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"Le fichier SQL {sql_file_path} est introuvable.")
    
    with open(sql_file_path, 'r') as f:
        sql = f.read()
    
    # Exécution du SQL
    hook.run(sql, autocommit=True)
    print("Table 'daily_subscription' créée avec succès !")

with DAG(
    dag_id="create_daily_subscription_table",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # exécution manuelle
    catchup=False,
    tags=["sqlserver", "init"],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )
