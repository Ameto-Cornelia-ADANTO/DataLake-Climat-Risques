markdown
# 🌍 DataLake Climat & Risques Naturels  
**Plateforme conteneurisée d'analyse de données climatiques et géologiques**  
*Ingestion, traitement et visualisation de données NOAA et USGS avec Hadoop, Spark et Streamlit*

## 🏗️ Structure du Projet
datalake-climate-risks/
├── docker-compose.yml # Configuration Docker
├── frontend/ # Dashboard Streamlit
│ ├── Dockerfile.streamlit # Image Docker pour Streamlit
│ ├── requirements.txt # Dépendances Python
│ ├── app.py # Application principale
│ ├── pages/ # Pages du dashboard
│ │ ├── 1_🏠_Dashboard.py # Page d'accueil
│ │ ├── 2_📊_NOAA_Visualisations.py # Visualisations NOAA
│ │ ├── 3_🌋_USGS_Visualisations.py # Visualisations USGS
│ │ ├── 4_🚨_Alertes_Temps_Réel.py # Alertes en temps réel
│ │ ├── 5_📁_Explorer_HDFS.py # Explorateur de données
│ │ └── 6_⚙️_Administration.py # Administration système
│ └── utils/ # Utilitaires
│ ├── hdfs_client.py # Client HDFS
│ └── spark_client.py # Client Spark
├── ingestion/ # Scripts d'ingestion
│ ├── batch_noaa.py # Ingestion batch NOAA
│ ├── batch_usgs.py # Ingestion batch USGS
│ └── stream_ingest.py # Ingestion streaming
├── spark-jobs/ # Jobs Spark
│ ├── etl_cleaning.py # Nettoyage ETL
│ ├── daily_aggregation.py # Agrégations quotidiennes
│ ├── detect_anomalies.py # Détection d'anomalies
│ └── calculate_trends.py # Calcul des tendances
├── notebooks/ # Notebooks Jupyter
│ └── development.ipynb # Notebook de développement
└── README.md # Documentation


## 🚀 Démarrage Rapide

### 1. Prérequis
- **Docker** et **Docker Compose** (Docker Desktop recommandé)
- **4 Go de RAM** minimum, 8 Go recommandés
- **Git**

### 2. Installation
```bash
# Clonez le dépôt
git clone <votre-repo>
cd Datalake_Projet

# Lancez tous les services
docker-compose up -d

# Attendez que tous les services soient prêts (30-60 secondes)
docker-compose logs -f
3. Initialisation des Données
bash
# Ingestion initiale des données NOAA
docker-compose exec spark spark-submit /app/ingestion/batch_noaa.py

# Ingestion initiale des données USGS
docker-compose exec spark spark-submit /app/ingestion/batch_usgs.py

# Lancement de l'ingestion streaming
docker-compose exec spark spark-submit /app/ingestion/stream_ingest.py
🖥️ Services Disponibles
Service	URL	Port	Description
📊 Streamlit Dashboard	http://localhost:8501	8501	Interface principale
⚡ Apache Spark Master	http://localhost:8080	8080	Interface Spark
🗄️ Hadoop HDFS NameNode	http://localhost:9870	9870	Explorateur HDFS
💻 Jupyter Lab	http://localhost:8888	8888	Environnement dev
🔄 Apache Airflow	http://localhost:8080	8080	Orchestration (si configuré)
📊 Fonctionnalités du Dashboard
🏠 Page Dashboard
Vue d'ensemble des KPIs climatiques

Carte interactive des risques

Statistiques globales

📊 NOAA Visualisations
Données météorologiques historiques

Graphiques de température et précipitations

Tendances climatiques par région

🌋 USGS Visualisations
Activité sismique en temps réel

Données hydrologiques

Surveillance géologique

🚨 Alertes Temps Réel
Alertes NOAA (tempêtes, inondations)

Alertes USGS (séismes > magnitude 4.5)

Notifications configurables par email

📁 Explorer HDFS
Navigation dans l'arborescence HDFS

Prévisualisation des fichiers Parquet/CSV

Téléchargement d'échantillons

⚙️ Administration
Monitoring des services

Gestion des pipelines ETL

Configuration des sources de données

🔄 Pipeline de Données
Architecture de traitement
text
Sources externes (NOAA/USGS APIs)
        ↓
Ingestion (batch + streaming)
        ↓
Stockage HDFS (/raw/)
        ↓
Traitement Spark (ETL/agrégation)
        ↓
Stockage HDFS (/processed/)
        ↓
Visualisation Streamlit
Jobs Spark disponibles
ETL Cleaning (etl_cleaning.py) - Nettoyage des données brutes

Daily Aggregation (daily_aggregation.py) - Agrégations temporelles

Anomaly Detection (detect_anomalies.py) - Détection automatique

Trend Analysis (calculate_trends.py) - Analyse des tendances

🛠️ Développement
Ajouter une nouvelle page au dashboard
python
# 1. Créez un fichier dans frontend/pages/
# 2. Nommez-le : "7_📈_Nouvelle_Analyse.py"
# 3. Structure de base :
import streamlit as st

st.set_page_config(page_title="Nouvelle Analyse")
st.title("📈 Nouvelle Analyse")
# Votre code ici
Exécuter un job Spark personnalisé
bash
docker-compose exec spark spark-submit /app/spark-jobs/votre_job.py
Accéder aux notebooks Jupyter
Ouvrez http://localhost:8888

Utilisez le token : jupyter

Les données sont accessibles dans /app/

⚙️ Configuration

# Configuration HDFS
HDFS_NAMENODE=hdfs://namenode:9000
HDFS_USER=datalake_user

# Configuration Spark
SPARK_MASTER=spark://spark-master:7077
Personnaliser les services
Éditez docker-compose.yml pour :

Ajuster les ressources mémoire/CPU

Modifier les ports exposés

Ajouter de nouveaux services

📁 Gestion des Données
Structure HDFS recommandée
text
/user/datalake/
├── raw/
│   ├── noaa/              # Données brutes NOAA
│   └── usgs/              # Données brutes USGS
├── processed/
│   ├── noaa/              # Données nettoyées NOAA
│   └── usgs/              # Données nettoyées USGS
├── analytics/             # Résultats d'analyses
└── models/               # Modèles ML entraînés
Commandes HDFS utiles
bash
# Lister les fichiers
docker-compose exec namenode hdfs dfs -ls /user/datalake

# Créer un dossier
docker-compose exec namenode hdfs dfs -mkdir -p /user/datalake/raw/noaa

# Copier des fichiers locaux vers HDFS
docker-compose exec namenode hdfs dfs -put localfile.csv /user/datalake/raw/
🧹 Maintenance
Commandes Docker essentielles
bash
# Voir l'état des services
docker-compose ps

# Voir les logs
docker-compose logs -f streamlit
docker-compose logs -f spark

# Arrêter proprement
docker-compose down

# Redémarrer un service
docker-compose restart spark
Nettoyage
bash
# Supprimer les conteneurs et volumes
docker-compose down -v

# Nettoyer les images non utilisées
docker system prune -a
⚠️ Dépannage
Problème	Solution
Port déjà utilisé	Modifiez les ports dans docker-compose.yml
HDFS non accessible	docker-compose restart namenode datanode
Spark jobs échouent	Vérifiez les logs : docker-compose logs spark
Dashboard lent	Augmentez les ressources dans docker-compose.yml
Erreurs d'API	Vérifiez les clés API dans .env
📚 Documentation Technique
Bibliothèques principales
Streamlit : Interface utilisateur

PySpark : Traitement distribué

HDFS : Stockage distribué

Pandas/NumPy : Analyse de données

Plotly/Matplotlib : Visualisations

Sources de données
NOAA : https://www.ncdc.noaa.gov/cdo-web/

USGS : https://earthquake.usgs.gov/fdsnws/event/1/

Documentation API : Voir les scripts dans ingestion/

📄 Licence
MIT License - Voir le fichier LICENSE pour plus de détails.