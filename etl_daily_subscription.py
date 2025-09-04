from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import pandas as pd
import os
import glob
import pyodbc
import logging

FILES_DIR = "/opt/airflow/files"   # chemin dans le container
LOADED_DIR = os.path.join(FILES_DIR, "loaded")
CHUNK_SIZE = 10000
os.makedirs(LOADED_DIR, exist_ok=True)


def etl_load_files():
    logging.info("🔹 Démarrage du traitement des fichiers Excel")

    # Connexion via Airflow
    hook = MsSqlHook(mssql_conn_id='mssql_conn')

    # Récupérer paramètres de connexion
    conn_params = hook.get_connection('mssql_conn')
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conn_params.host};"
        f"DATABASE=activation_DB;"
        f"UID={conn_params.login};"
        f"PWD={conn_params.password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Vérifier fichiers déjà chargés
    loaded_files_query = "SELECT NameFile FROM LOG_DB.dbo.subscription_log;"
    loaded_files = hook.get_pandas_df(loaded_files_query)['NameFile'].tolist()

    # Lister les fichiers disponibles
    all_files = glob.glob(os.path.join(FILES_DIR, "subscribtion_*.xlsx"))
    if not all_files:
        logging.warning("⚠️ Aucun fichier trouvé.")
        return

    for file_path in all_files:
        file_name = os.path.basename(file_path)
        if file_name in loaded_files:
            logging.info(f"⏩ Déjà chargé : {file_name}")
            continue

        # Charger le fichier Excel
        df = pd.read_excel(file_path)
        df['FileName'] = file_name
        df.fillna('', inplace=True)

        if 'date_souscription' in df.columns:
            df['date_souscription'] = pd.to_datetime(df['date_souscription'], errors='coerce')

        # Supprimer les lignes invalides
        df = df.dropna(subset=['date_souscription'])
        if df.empty:
            logging.warning(f"❌ Aucun enregistrement valide pour {file_name}")
            continue

        data = [tuple(row) for row in df.to_numpy()]

        # Insert SQL
        insert_query = """
        INSERT INTO dbo.daily_subscription
        (customer, lastname, firstname, sim, msisdn, libelleprofil, date_souscription,
         typeclient, identity_number, identity_desc, Etat_identification, Etat_ligne, FileName)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Insertion par chunks avec gestion des erreurs
        for i in range(0, len(data), CHUNK_SIZE):
            chunk = data[i:i+CHUNK_SIZE]
            try:
                cursor.executemany(insert_query, chunk)
                conn.commit()
                logging.info(f"✅ {len(chunk)} lignes insérées depuis {file_name}")
            except Exception as e:
                logging.error(f"❌ Erreur lors de l'insertion de {file_name}: {e}")
                conn.rollback()

        # Journaliser
        try:
            hook.run(f"""
                INSERT INTO LOG_DB.dbo.subscription_log (Loaded_date, NameFile)
                VALUES (GETDATE(), '{file_name}');
            """, autocommit=True)
            logging.info(f"📝 Journalisé : {file_name}")
        except Exception as e:
            logging.error(f"❌ Erreur lors du journal pour {file_name}: {e}")

        # Déplacer le fichier vers le dossier loaded
        dest_path = os.path.join(LOADED_DIR, file_name)
        os.rename(file_path, dest_path)
        logging.info(f"📂 Déplacé vers loaded : {file_name}")

    cursor.close()
    conn.close()
    logging.info("🎉 Tous les fichiers traités.")


# Définition du DAG
with DAG(
    dag_id="etl_load_files",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,   # Exécution manuelle ou '0 */2 * * *' pour toutes les 2h
    catchup=False,
    tags=["mssql", "etl", "files"],
) as dag:

    load_files_task = PythonOperator(
        task_id="load_files_task",
        python_callable=etl_load_files,
    )
