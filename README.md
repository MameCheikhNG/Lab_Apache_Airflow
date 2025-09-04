# Lab_Apache_Airflow
# Airflow Lab – SQL Server ETL

Ce projet montre comment utiliser Apache Airflow avec SQL Server pour créer un pipeline ETL simple :
- Lecture de données brutes
- Transformation (nettoyage, suppression des doublons, mise en majuscules)
- Chargement dans une table propre

## Structure
- `dags/etl_customers.py` : le DAG principal
- `sql/create_tables.sql` : script de création des tables
