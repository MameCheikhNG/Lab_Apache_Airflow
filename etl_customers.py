from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import pandas as pd

def transform_data():
    hook = MsSqlHook(mssql_conn_id="mssql_conn", schema="activation_DB")

    # Lire données brutes
    df = hook.get_pandas_df("SELECT * FROM raw_customers")

    # Transformation : majuscules + suppression doublons
    df['firstname'] = df['firstname'].str.upper()
    df['lastname'] = df['lastname'].str.upper()
    df = df.drop_duplicates(subset=['email'])

    # Nettoyer la table clean_customers
    hook.run("TRUNCATE TABLE clean_customers", autocommit=True)

    # Charger dans la table clean_customers
    rows = df.to_records(index=False).tolist()
    hook.insert_rows(
        table="clean_customers",
        rows=rows,
        target_fields=["id", "firstname", "lastname", "email"]
    )

with DAG(
    dag_id="etl_customers",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "sqlserver"],
) as dag:

    create_tables = MsSqlOperator(
        task_id="create_tables",
        mssql_conn_id="mssql_conn",
        sql="sql/create_tables.sql"
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    create_tables >> transform
