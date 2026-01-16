#!/bin/bash
# Script d'initialisation HDFS pour DataLake Climat & Risques Naturels

echo "🚀 Initialisation HDFS pour DataLake Climat & Risques Naturels"
echo "=============================================================="

# Attendre que HDFS soit prêt
echo "⏳ Attente du démarrage de HDFS..."
sleep 10

# Créer la structure de base
echo "📁 Création de la structure HDFS..."

docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/raw
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/raw/noaa
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/raw/usgs
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/silver
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/silver/noaa_cleaned
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/silver/usgs_cleaned
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/gold
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/gold/daily_aggregates
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/gold/anomalies
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/gold/trends
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/alerts
docker exec namenode hdfs dfs -mkdir -p /hadoop-climate-risk/metadata
docker exec namenode hdfs dfs -mkdir -p /tmp/spark-checkpoints
docker exec namenode hdfs dfs -mkdir -p /tmp/spark-events

# Définir les permissions
echo "🔒 Configuration des permissions..."
docker exec namenode hdfs dfs -chmod -R 777 /hadoop-climate-risk
docker exec namenode hdfs dfs -chmod -R 777 /tmp

# Vérifier la structure
echo "🔍 Vérification de la structure..."
docker exec namenode hdfs dfs -ls -R /hadoop-climate-risk

# Créer des fichiers de test
echo "🧪 Création de fichiers de test..."
cat > /tmp/test_noaa.json << EOF
{"date": "2024-01-01", "station_id": "TEST001", "temperature": 15.5, "precipitation": 0.0}
{"date": "2024-01-02", "station_id": "TEST001", "temperature": 16.2, "precipitation": 2.5}
{"date": "2024-01-03", "station_id": "TEST001", "temperature": 14.8, "precipitation": 0.0}
EOF

cat > /tmp/test_usgs.json << EOF
{"timestamp": "2024-01-01T12:00:00", "magnitude": 4.5, "latitude": 34.05, "longitude": -118.25}
{"timestamp": "2024-01-02T08:30:00", "magnitude": 3.2, "latitude": 36.17, "longitude": -120.72}
EOF

# Copier les fichiers de test vers HDFS
docker cp /tmp/test_noaa.json namenode:/tmp/
docker cp /tmp/test_usgs.json namenode:/tmp/
docker exec namenode hdfs dfs -put /tmp/test_noaa.json /hadoop-climate-risk/raw/noaa/
docker exec namenode hdfs dfs -put /tmp/test_usgs.json /hadoop-climate-risk/raw/usgs/

# Nettoyer
rm -f /tmp/test_noaa.json /tmp/test_usgs.json

# Afficher le résumé
echo ""
echo "✅ INITIALISATION TERMINÉE"
echo "=========================="
echo "🌐 HDFS Web UI: http://localhost:9870"
echo "📁 Structure créée:"
echo "   /hadoop-climate-risk/raw/noaa/        # Données brutes NOAA"
echo "   /hadoop-climate-risk/raw/usgs/        # Données brutes USGS"
echo "   /hadoop-climate-risk/silver/          # Données nettoyées"
echo "   /hadoop-climate-risk/gold/            # Données agrégées"
echo "   /hadoop-climate-risk/alerts/          # Alertes temps réel"
echo "   /hadoop-climate-risk/metadata/        # Métadonnées"
echo ""
echo "📊 Fichiers de test créés:"
docker exec namenode hdfs dfs -ls /hadoop-climate-risk/raw/noaa/
docker exec namenode hdfs dfs -ls /hadoop-climate-risk/raw/usgs/