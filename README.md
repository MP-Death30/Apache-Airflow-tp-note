# README.md

## Auteur
- Nom : MP-Death30
- Formation : MIA26.2
- Date : 13-04-2026

## Prérequis
Docker Desktop >=4.0
Docker Compose >=2.0
Python >=3.11 (pour les tests locaux)

## Instructions de déploiement

### 1. Démarrage de la stack
Création de l'arborescence locale et définition des permissions :
```bash
mkdir -p data/raw data/indicateurs data/rapports
echo "AIRFLOW_UID=$(id -u)" > .env
```
Lancement des conteneurs en arrière-plan :
```bash
docker compose up -d
```
Vérification de l'état des services :
```bash
docker compose ps
```

### 2. Configuration des connexions et variables Airflow
Accéder à l'interface Airflow (http://localhost:8080) avec les identifiants `admin` / `admin`.

Créer la connexion PostgreSQL (Admin > Connections > +) :
- Connection Id : postgres_ars
- Connection Type : Postgres
- Host : postgres-ars
- Schema : ars_epidemio
- Login : postgres
- Password : postgres
- Port : 5432

Créer les variables (Admin > Variables > +) :
- semaines_historique : 12
- seuil_alerte_incidence : 150
- seuil_urgence_incidence : 500
- seuil_alerte_zscore : 1.5
- seuil_urgence_zscore : 3.0
- departements_occitanie : ["09","11","12","30","31","32","34","46","48","65","66","81","82"]
- syndromes_surveilles : ["GRIPPE", "GEA", "SG", "BRONCHIO", "COVID19"]
- archive_base_path : /data/ars

### 3. Démarrage du pipeline
Dans l'interface Airflow (Dags) :
1. Activer le DAG `ars_epidemio_dag` en basculant le commutateur (unpause).
2. Déclencher une exécution manuelle via le bouton "Trigger DAG" (Play).
3. Suivre l'exécution dans la vue Graph ou Grid.

## Architecture des données
Partitionnement local (Bind Mount) : Les données brutes sont archivées selon une hiérarchie temporelle stricte (`/data/ars/raw/YYYY/SXX/`). Les indicateurs et rapports finaux disposent de dossiers dédiés pour isolation.
Schéma PostgreSQL : Architecture relationnelle (base `ars_epidemio`). Les référentiels géographiques (`departements`) et médicaux (`syndromes`) sont croisés par les tables transactionnelles (`donnees_hebdomadaires`, `indicateurs_epidemiques`, `rapports_ars`). Traçabilité assurée par les champs `created_at` et `updated_at`.

## Décisions techniques
Idempotence SQL : Remplacement de `CREATE OR REPLACE TRIGGER` par `DROP TRIGGER IF EXISTS` suivi d'un `CREATE TRIGGER` pour assurer la compatibilité PostgreSQL 13 lors des réexécutions.
Gestion des volumes : Remplacement du volume Docker persistant initial par un bind mount (`./data:/data/ars`) pour octroyer les droits d'écriture à l'utilisateur non privilégié Airflow (UID 50000) et résoudre l'erreur `[Errno 13] Permission denied`.
Correction logique métier : Réécriture de l'étape d'évaluation épidémique (Etape 8) avec une agrégation sur `syndrome` au lieu de `code_dept`, ce dernier étant absent de la table cible selon le schéma fourni.
Rattrapage temporel : Modification du paramètre `start_date` à `2024-04-01` pour correspondre à la réalité des datasets distants et éviter l'échec du backfilling.

## Difficultés rencontrées et solutions
Échec d'écriture dans le volume partagé causé par l'attribution des droits `root` par le daemon Docker. Résolu par un basculement sur un bind mount local pré-instancié.
Erreurs de syntaxe SQL fatales bloquant l'initialisation de la base, générées par des retours à la ligne erratiques dans le copier-coller des commentaires SQL du sujet. Résolu par le nettoyage strict du fichier d'initialisation.
Incohérence du cahier des charges entre la structure imposée pour la table `indicateurs_epidemiques` et la requête d'évaluation d'urgence (colonne manquante). Résolu par adaptation de la requête d'évaluation.
