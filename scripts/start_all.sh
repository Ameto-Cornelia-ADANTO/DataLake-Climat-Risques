#!/bin/bash
# Script de démarrage complet pour DataLake

echo "🌍 DATA LAKE CLIMAT & RISQUES NATURELS"
echo "========================================"

# 1. Démarrer Docker Compose
echo "1. 🐳 Démarrage des conteneurs Docker..."
docker-compose up -d

# 2. Initialiser HDFS
echo "2. 📁 Initialisation HDFS..."
sleep 20
./scripts/init_hdfs.sh

# 3. Configurer Kafka
echo "3. 📡 Configuration Kafka..."
sleep 10
./scripts/create_kafka_topic.sh

# 4. Vérifier les services
echo "4. 🔍 Vérification des services..."
echo ""
echo "📊 SERVICES DISPONIBLES:"
echo "   • HDFS UI:          http://localhost:9870"
echo "   • Spark Master UI:  http://localhost:8080"
echo "   • Spark History:    http://localhost:18080"
echo "   • Kafka UI:         http://localhost:8081"
echo "   • Streamlit:        http://localhost:8501"
echo "   • Jupyter:          http://localhost:8888"
echo ""

# 5. Lancer un job Spark de test
echo "5. 🧪 Lancement job Spark de test..."
sleep 5
docker exec spark-master spark-submit \
  --master spark://