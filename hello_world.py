from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Définition du DAG
with DAG(
    dag_id="hello_world",
   start_date=datetime(2025, 9, 1, 17, 0),  # La première exécution sera le 1er sept à 17h00
    schedule_interval="0 17 * * *",          # Tous les jours à 17h00 schedule_interval="@daily",  # planification (1 fois par jour)
    catchup=False,
    tags=["mssql", "etl", "files"],
) as dag:

    # Tâche simple qui affiche "Hello World"
    task_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello World depuis Airflow !"'
    )
