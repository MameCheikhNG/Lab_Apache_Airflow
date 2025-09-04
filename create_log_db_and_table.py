from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def create_log_db_and_table():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    
    # 1️⃣ Création de la base LOG_DB
    hook.run("CREATE DATABASE LOG_DB;", autocommit=True)
    print("Base de données 'LOG_DB' créée avec succès !")
    
    # 2️⃣ Création de la table subscription_log
    sql_create_table = """
    USE LOG_DB;
    IF OBJECT_ID('dbo.subscription_log', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.subscription_log (
            Loaded_date DATETIME NOT NULL,
            NameFile NVARCHAR(255) NOT NULL
        );
    END
    """
    hook.run(sql_create_table, autocommit=True)
    print("Table 'subscription_log' créée avec succès dans LOG_DB !")

# Définition du DAG
with DAG(
    dag_id="create_log_db_and_table",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # exécution manuelle
    catchup=False,
    tags=["sqlserver", "init"],
) as dag:

    create_log_db_task = PythonOperator(
        task_id='create_log_db_and_table',
        python_callable=create_log_db_and_table
    )
