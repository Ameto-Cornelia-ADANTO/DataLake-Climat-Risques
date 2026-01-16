@echo off
chcp 65001 >nul
echo ============================================
echo    DEPLOIEMENT DATALAKE - CLIMAT & RISQUES
echo ============================================
echo.

echo [1/7] Nettoyage des anciens conteneurs...
docker-compose down -v 2>nul
docker rm -f namenode datanode1 spark-master spark-worker zookeeper kafka kafka-ui streamlit jupyter spark-history 2>nul

echo [2/7] Creation de la structure...
if not exist frontend mkdir frontend
if not exist frontend\pages mkdir frontend\pages
if not exist frontend\utils mkdir frontend\utils
if not exist data mkdir data
if not exist ingestion mkdir ingestion
if not exist spark-jobs mkdir spark-jobs
if not exist notebooks mkdir notebooks
if not exist spark-events mkdir spark-events

echo [3/7] Creation des fichiers essentiels...
if not exist requirements.txt (
  echo streamlit==1.28.0 > requirements.txt
  echo pandas==2.0.3 >> requirements.txt
  echo numpy==1.24.3 >> requirements.txt
  echo plotly==5.17.0 >> requirements.txt
  echo requests==2.31.0 >> requirements.txt
  echo kafka-python==2.0.2 >> requirements.txt
  echo pyarrow==14.0.1 >> requirements.txt
)

if not exist frontend\app.py (
  echo import streamlit as st > frontend\app.py
  echo. >> frontend\app.py
  echo st.set_page_config(page_title="DataLake", layout="wide") >> frontend\app.py
  echo st.title("ðŸŒ DataLake Climat & Risques Naturels") >> frontend\app.py
  echo st.success("ðŸ‘Œ Systeme en cours de demarrage...") >> frontend\app.py
)

echo [4/7] Demarrage des services...
docker-compose up -d

echo [5/7] Attente du demarrage (45 secondes)...
timeout /t 45 /nobreak >nul

echo [6/7] Initialisation HDFS...
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/raw/noaa 2>nul
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/raw/usgs 2>nul
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/silver 2>nul
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/gold 2>nul
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/alerts 2>nul
docker exec namenode hdfs dfs -chmod -R 777 /hadoop-climate-risk 2>nul

echo [7/7] Creation du topic Kafka...
docker exec kafka kafka-topics --create --topic climate-alerts --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092 2>nul

echo.
echo ============================================
echo        ðŸ‘Œ DEPLOIEMENT REUSSI !
echo ============================================
echo.
echo ðŸŒ INTERFACES DISPONIBLES :
echo.
echo   â€¢ Dashboard Streamlit:   http://localhost:8501
echo   â€¢ HDFS Web UI:           http://localhost:9870
echo   â€¢ Spark Master UI:       http://localhost:8080
echo   â€¢ Kafka UI:              http://localhost:8081
echo   â€¢ Jupyter Notebook:      http://localhost:8888
echo   â€¢ Spark History:         http://localhost:18080
echo.
echo ðŸ“Š POUR VERIFIER :
echo   docker ps
echo   curl http://localhost:8501
echo.
echo ============================================
pause