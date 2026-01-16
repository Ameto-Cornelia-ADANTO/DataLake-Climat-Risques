#!/bin/bash
# Script pour créer les topics Kafka pour DataLake

echo "🚀 Configuration Kafka pour DataLake Climat & Risques Naturels"
echo "=============================================================="

# Attendre que Kafka soit prêt
echo "⏳ Attente du démarrage de Kafka..."
sleep 15

# Créer les topics
echo "📡 Création des topics Kafka..."

# Topic pour les alertes temps réel
docker exec kafka kafka-topics --create \
  --topic climate-alerts \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Topic pour les logs d'ingestion
docker exec kafka kafka-topics --create \
  --topic ingestion-logs \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Topic pour les métriques
docker exec kafka kafka-topics --create \
  --topic system-metrics \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Vérifier les topics créés
echo "🔍 Liste des topics Kafka:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Créer un producer de test
echo "🧪 Test de production de message..."
docker exec kafka bash -c "
echo '{\"test\": true, \"message\": \"Kafka configuré pour DataLake\", \"timestamp\": \"$(date -Iseconds)\"}' | \
kafka-console-producer --topic climate-alerts --bootstrap-server localhost:9092
"

# Consommer le message de test
echo "📥 Test de consommation de message..."
docker exec kafka timeout 5 kafka-console-consumer \
  --topic climate-alerts \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 1

echo ""
echo "✅ KAFKA CONFIGURÉ AVEC SUCCÈS"
echo "==============================="
echo "🌐 Kafka UI: http://localhost:8081"
echo "📡 Topics créés:"
echo "   • climate-alerts    # Alertes temps réel"
echo "   • ingestion-logs    # Logs d'ingestion"
echo "   • system-metrics    # Métriques système"
echo ""
echo "🔧 Commandes utiles:"
echo "   Consommer un topic: docker exec kafka kafka-console-consumer --topic climate-alerts --bootstrap-server localhost:9092"
echo "   Produire un message: docker exec kafka kafka-console-producer --topic climate-alerts --bootstrap-server localhost:9092"
echo "   Voir les offsets: docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list"