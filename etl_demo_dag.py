from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="etl_demo",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",   # le DAG s’exécute tous les jours
    catchup=False,                # évite de rattraper les runs passés
    tags=["etl"],                 # pratique pour organiser dans l’UI
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command='echo "Extraction des données"'
    )

    transform = BashOperator(
        task_id="transform",
        bash_command='echo "Transformation des données"'
    )

    load = BashOperator(
        task_id="load",
        bash_command='echo "Chargement des données dans SQL Server"'
    )

    # Définition de la séquence d’exécution
    extract >> transform >> load
