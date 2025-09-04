from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def create_database():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    # autocommit=True pour éviter l'erreur de transaction
    hook.run("CREATE DATABASE activation_DB;", autocommit=True)
    print("Base de données 'activation_DB' créée avec succès !")

with DAG(
    dag_id="create_activation_db",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sqlserver", "init"],
) as dag:

    create_db_task = PythonOperator(
        task_id='create_database',
        python_callable=create_database
    )
