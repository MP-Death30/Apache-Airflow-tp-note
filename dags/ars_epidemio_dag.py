from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os, shutil, json


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


def calculer_indicateurs_epidemiques(**context):
    semaine = (
        f"{context['execution_date'].year}-"
        f"S{context['execution_date'].isocalendar()[1]:02d}"
    )
    annee, num_sem = semaine.split("-")
    chemin_archive = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    
    with open(chemin_archive, "r") as f:
        donnees_json = json.load(f)["syndromes"]

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import (
        calculer_zscore, classifier_statut_ias,
        classifier_statut_zscore, classifier_statut_final,
        calculer_r0_simplifie
    )

    hook = PostgresHook(postgres_conn_id="postgres_ars")
    indicateurs = []
    durees_infectieuses = {"GRIPPE": 5, "GEA": 3, "SG": 5, "BRONCHIO": 7, "COVID19": 7}

    for syndrome, data in donnees_json.items():
        valeur_ias = data.get("valeur_ias")
        if valeur_ias is None:
            continue

        sql = """
            SELECT valeur_ias FROM donnees_hebdomadaires
            WHERE syndrome = %s AND semaine < %s
            ORDER BY semaine DESC LIMIT 3;
        """
        records = hook.get_records(sql, parameters=(syndrome, semaine))
        valeurs_historiques = [r[0] for r in records]
        valeurs_historiques.reverse()
        serie_hebdomadaire = valeurs_historiques + [valeur_ias]

        historique_saisons = list(data.get("historique", {}).values())
        z_score = calculer_zscore(valeur_ias, historique_saisons)
        r0 = calculer_r0_simplifie(serie_hebdomadaire, durees_infectieuses.get(syndrome, 5))
        
        statut_ias = classifier_statut_ias(valeur_ias, data.get("seuil_min"), data.get("seuil_max"))
        statut_zscore = classifier_statut_zscore(z_score)
        statut_final = classifier_statut_final(statut_ias, statut_zscore)

        indicateurs.append({
            "code_dept": "76",
            "semaine": semaine,
            "syndrome": syndrome,
            "taux_incidence": valeur_ias,
            "z_score": z_score,
            "r0_estime": r0,
            "nb_annees_reference": len([v for v in historique_saisons if v is not None]),
            "statut": statut_final
        })

    os.makedirs("/data/ars/indicateurs", exist_ok=True)
    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json", "w", encoding="utf-8") as f:
        json.dump(indicateurs, f, ensure_ascii=False, indent=2)


def inserer_donnees_postgres(**context) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import json

    semaine: str = context["templates_dict"]["semaine"]
    annee, num_sem = semaine.split("-")
    
    with open(f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json") as f:
        donnees_brutes = json.load(f)["syndromes"]
        
    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    sql_donnees = """
        INSERT INTO donnees_hebdomadaires
        (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias = EXCLUDED.valeur_ias,
            seuil_min_saison = EXCLUDED.seuil_min_saison,
            seuil_max_saison = EXCLUDED.seuil_max_saison,
            nb_jours_donnees = EXCLUDED.nb_jours_donnees;
    """

    sql_indicateurs = """
        INSERT INTO indicateurs_epidemiques
        (semaine, syndrome, valeur_ias, z_score, r0_estime, nb_saisons_reference, statut)
        VALUES (%(semaine)s, %(syndrome)s, %(taux_incidence)s, %(z_score)s, %(r0_estime)s, %(nb_annees_reference)s, %(statut)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias = EXCLUDED.valeur_ias,
            z_score = EXCLUDED.z_score,
            r0_estime = EXCLUDED.r0_estime,
            nb_saisons_reference = EXCLUDED.nb_saisons_reference,
            statut = EXCLUDED.statut;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for syndrome, data in donnees_brutes.items():
                if data.get("valeur_ias") is not None:
                    cur.execute(sql_donnees, data)
                    
            for ind in indicateurs:
                cur.execute(sql_indicateurs, ind)
        conn.commit()



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

    calculer = PythonOperator(
        task_id="calculer_indicateurs_epidemiques",
        python_callable=calculer_indicateurs_epidemiques,
        provide_context=True,
    )

    inserer_postgres = PythonOperator(
        task_id="inserer_donnees_postgres",
        python_callable=inserer_donnees_postgres,
        templates_dict={
            "semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}"
        },
        provide_context=True,
    )

    collecter_sursaud >> archiver >> verifier >> calculer >> inserer_postgres