from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os, shutil


default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

def collecter_donnees_ias(**context) -> str:
    """Télécharge les CSV IAS® et retourne le chemin du fichier JSON créé."""
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"
    
    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_sursaud import (
        DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
        agreger_semaine, sauvegarder_donnees
    )
    
    resultats = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
        
    return sauvegarder_donnees(resultats, semaine, output_dir)


def archiver_local(**context) -> str:
    """
    Organise le fichier brut dans la structure d'archivage
    partitionnée.
    """
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    
    # Récupération du chemin depuis XCom
    chemin_source = context["task_instance"].xcom_pull(
        task_ids="collecter_donnees_sursaud"
    )
    
    archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)
    chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
    
    shutil.copy2(chemin_source, chemin_dest)
    print(f"ARCHIVE_OK: {chemin_dest}")
    return chemin_dest

def verifier_archive(**context) -> bool:
    """Vérifie que le fichier d'archive existe et n'est pas vide."""
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    
    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante {chemin}")
    taille = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide: {chemin}")
        
    print(f"ARCHIVE_VALIDE: {chemin} ({taille} octets)")
    return True




with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 06 * * 1",
    start_date=datetime(2024, 1, 1), # Tous les lundis à 6h UTC
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:

    # init_base_donnees = PostgresOperator(
    #     task_id="init_base_donnees",
    #     postgres_conn_id="postgres_ars",
    #     sql="sql/init_ars_epidemio.sql", # Fichier SQL séparé dans le dossier dags/sql/
    #     autocommit=True,
    # )

    collecter_sursaud = PythonOperator(
        task_id="collecter_donnees_sursaud",
        python_callable=collecter_donnees_ias,
        provide_context=True,
    )

    archiver = PythonOperator(
        task_id="archiver_local",
        python_callable=archiver_local,
        provide_context=True,
    )

    verifier = PythonOperator(
        task_id="verifier_archive",
        python_callable=verifier_archive,
        provide_context=True,
    )

    #init_base_donnees >> collecter_sursaud >> archiver >> verifier
    collecter_sursaud >> archiver >> verifier