from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
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
    if chemin_source is None:
        raise ValueError("XCom vide : la tâche collecter_donnees_sursaud n'a pas retourné de chemin")
    
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
                    data_sql = data.copy()
                    data_sql["semaine"] = semaine
                    data_sql["syndrome"] = syndrome
                    data_sql["seuil_min"] = data.get("seuil_min")
                    data_sql["seuil_max"] = data.get("seuil_max")
                    data_sql["nb_jours"] = data.get("nb_jours")
                    cur.execute(sql_donnees, data_sql)
                    
            for ind in indicateurs:
                cur.execute(sql_indicateurs, ind)
        conn.commit()


logger = logging.getLogger(__name__)

def evaluer_situation_epidemique(**context) -> str:
    semaine: str = context["templates_dict"]["semaine"]
    hook = PostgresHook(postgres_conn_id="postgres_ars")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(DISTINCT syndrome), ARRAY_AGG(DISTINCT syndrome)
                FROM indicateurs_epidemiques
                WHERE semaine = %s GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: {"nb": row[1], "cibles": row[2]} for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", {}).get("nb", 0)
    nb_alerte = resultats.get("ALERTE", {}).get("nb", 0)

    context["task_instance"].xcom_push(key="urgences", value=resultats.get("URGENCE", {}).get("cibles", []))
    context["task_instance"].xcom_push(key="alertes", value=resultats.get("ALERTE", {}).get("cibles", []))

    if nb_urgence > 0: return "declencher_alerte_ars"
    elif nb_alerte > 0: return "envoyer_bulletin_surveillance"
    else: return "confirmer_situation_normale"

def declencher_alerte_ars(**context):
    cibles = context["task_instance"].xcom_pull(task_ids="evaluer_situation_epidemique", key="urgences")
    logger.critical(f"ALERTE ARS DÉCLENCHÉE - Syndromes : {cibles}")

def envoyer_bulletin_surveillance(**context):
    cibles = context["task_instance"].xcom_pull(task_ids="evaluer_situation_epidemique", key="alertes")
    logger.warning(f"Bulletin envoyé - ALERTE - Syndromes : {cibles}")

def confirmer_situation_normale(**context):
    logger.info("Situation normale, aucune action.")

def generer_rapport_hebdomadaire(**context) -> None:
    semaine: str = context["templates_dict"]["semaine"]
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT syndrome, valeur_ias, z_score, r0_estime, statut
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                ORDER BY statut DESC, valeur_ias DESC
            """, (semaine,))
            indicateurs = cur.fetchall()

    statuts = [row[4] for row in indicateurs]

    if "URGENCE" in statuts: situation_globale = "URGENCE"
    elif "ALERTE" in statuts: situation_globale = "ALERTE"
    else: situation_globale = "NORMAL"

    cibles_urgence = [row[0] for row in indicateurs if row[4] == "URGENCE"]
    cibles_alerte = [row[0] for row in indicateurs if row[4] == "ALERTE"]

    recommandations_par_niveau = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Renforcement des équipes de surveillance dans les services d'urgences",
            "Communication renforcée auprès des professionnels de santé libéraux",
            "Notification immédiate à Santé Publique France et au Ministère de la Santé",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de santé",
            "Vérification des capacités d'accueil des services d'urgences",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport = {
        "semaine": semaine,
        "region": "Occitanie",
        "code_region": "76",
        "date_generation": datetime.utcnow().isoformat(),
        "situation_globale": situation_globale,
        "syndromes_en_urgence": cibles_urgence,
        "syndromes_en_alerte": cibles_alerte,
        "indicateurs": [
            {
                "syndrome": row[0],
                "taux_incidence_100k": row[1],
                "z_score": row[2],
                "ro_estime": row[3],
                "statut": row[4],
            }
            for row in indicateurs
        ],
        "recommandations": recommandations_par_niveau[situation_globale],
        "genere_par": "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    hook2 = PostgresHook(postgres_conn_id="postgres_ars")
    with hook2.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rapports_ars
                (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence, rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine) DO UPDATE SET
                situation_globale = EXCLUDED.situation_globale,
                nb_depts_alerte = EXCLUDED.nb_depts_alerte,
                nb_depts_urgence = EXCLUDED.nb_depts_urgence,
                rapport_json = EXCLUDED.rapport_json,
                chemin_local = EXCLUDED.chemin_local,
                updated_at = CURRENT_TIMESTAMP
            """, (
                semaine,
                situation_globale,
                len(cibles_alerte),
                len(cibles_urgence),
                json.dumps(rapport, ensure_ascii=False),
                local_path
            ))
        conn.commit()

    logger.info(f"Rapport {semaine} généré. Statut: {situation_globale}")



with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 06 * * 1",
    start_date=datetime(2024, 4, 1), # Tous les lundis à 6h UTC
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:

    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql", # Fichier SQL séparé dans le dossier dags/sql/
        autocommit=True,
    )

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

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        templates_dict={"semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}"},
        provide_context=True,
    )

    alerte_ars = PythonOperator(task_id="declencher_alerte_ars", python_callable=declencher_alerte_ars, provide_context=True)
    bulletin = PythonOperator(task_id="envoyer_bulletin_surveillance", python_callable=envoyer_bulletin_surveillance, provide_context=True)
    normale = PythonOperator(task_id="confirmer_situation_normale", python_callable=confirmer_situation_normale, provide_context=True)

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        templates_dict={
            "semaine": "{{ execution_date.year }}-S{{ '%02d' % execution_date.isocalendar()[1] }}"
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,
    )

    init_base_donnees >> collecter_sursaud >> archiver >> verifier >> calculer >> inserer_postgres
    inserer_postgres >> evaluer
    evaluer >> [alerte_ars, bulletin, normale] >> generer_rapport