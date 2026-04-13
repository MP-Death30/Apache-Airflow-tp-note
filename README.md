# README.md

## Auteur
- Nom : MP-Death30
- Formation : MIA26.2
- Date : 13-04-2026

## Prérequis
- Docker Desktop >=4.0
- Docker Compose >=2.0
- Python >=3.11 (pour les tests locaux)

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

- Idempotence des déclencheurs SQL : Remplacement de la syntaxe CREATE OR REPLACE TRIGGER (non reconnue par PostgreSQL 13) par un mécanisme destructif préalable DROP TRIGGER IF EXISTS suivi d'une création stricte.
- Résolution des droits d'écriture : Remplacement du volume Docker persistant classique par un bind mount pour garantir que l'utilisateur non-privilégié airflow (UID 50000) puisse générer et écrire les fichiers JSON sans lever d'erreur.
- Alignement de la granularité des données : Modification de la requête d'évaluation d'urgence (Étape 8). Le regroupement a été réorienté sur la colonne syndrome afin de pallier l'absence de la colonne code_dept dans le schéma de la table indicateurs_epidemiques imposé.
- Synchronisation temporelle : Modification du start_date du DAG à 2024-04-01 pour correspondre à la plage de disponibilité effective des fichiers CSV sources, évitant les échecs de collecte lors du rattrapage historique (catchup).

## Difficultés

- Conflit de propriété sur le dossier racine /data/ars initialisé implicitement par le daemon Docker en root.
- Rupture du flux d'exécution sur des erreurs de syntaxe SQL héritées des coupures de lignes dans les commentaires du code fourni.
- Désalignement entre les spécifications techniques du schéma (Section 5) et les requêtes fonctionnelles (Section 8) conduisant à des erreurs de ciblage de colonnes
