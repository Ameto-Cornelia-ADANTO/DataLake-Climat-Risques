#!/usr/bin/env python3
"""
Producer Kafka pour envoyer des alertes en temps réel
"""
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClimateAlertProducer:
    """Producteur d'alertes climatiques pour Kafka"""
    
    def __init__(self, bootstrap_servers='kafka:9092'):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            self.topic = 'climate-alerts'
            logger.info(f"✅ Producteur Kafka initialisé (topic: {self.topic})")
        except Exception as e:
            logger.error(f"❌ Erreur initialisation Kafka: {e}")
            sys.exit(1)
    
    def generate_alert(self, alert_id=None):
        """Génère une alerte simulée"""
        alert_types = [
            {"type": "Séisme", "icon": "🌋", "sources": ["USGS"]},
            {"type": "Tempête", "icon": "🌪️", "sources": ["NOAA"]},
            {"type": "Inondation", "icon": "🌊", "sources": ["NOAA", "USGS"]},
            {"type": "Vague de chaleur", "icon": "🔥", "sources": ["NOAA"]},
            {"type": "Incendie", "icon": "🚒", "sources": ["NASA", "NOAA"]}
        ]
        
        regions = [
            {"name": "Californie", "lat": 36.7783, "lon": -119.4179},
            {"name": "Alaska", "lat": 64.2008, "lon": -149.4937},
            {"name": "Floride", "lat": 27.6648, "lon": -81.5158},
            {"name": "Texas", "lat": 31.9686, "lon": -99.9018},
            {"name": "Québec", "lat": 52.9399, "lon": -73.5491},
            {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
            {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503}
        ]
        
        alert_type = random.choice(alert_types)
        region = random.choice(regions)
        
        # Générer des données spécifiques au type
        specific_data = {}
        if alert_type["type"] == "Séisme":
            specific_data = {
                "magnitude": round(random.uniform(2.0, 8.0), 1),
                "depth": round(random.uniform(1, 100), 1),
                "mag_type": random.choice(["ml", "md", "mwr"])
            }
        elif alert_type["type"] == "Tempête":
            specific_data = {
                "wind_speed": round(random.uniform(50, 200), 1),
                "pressure": round(random.uniform(950, 1020), 1),
                "category": random.choice(["Tropical", "Extratropical", "Convective"])
            }
        elif alert_type["type"] == "Vague de chaleur":
            specific_data = {
                "temperature": round(random.uniform(35, 50), 1),
                "duration_days": random.randint(3, 10),
                "humidity": round(random.uniform(30, 90), 1)
            }
        
        alert = {
            "alert_id": alert_id or f"alert_{int(time.time())}_{random.randint(1000, 9999)}",
            "type": alert_type["type"],
            "icon": alert_type["icon"],
            "severity": random.randint(1, 5),
            "region": region["name"],
            "latitude": region["lat"] + random.uniform(-2, 2),
            "longitude": region["lon"] + random.uniform(-2, 2),
            "description": f"Événement {alert_type['type'].lower()} détecté en {region['name']}",
            "timestamp": datetime.now().isoformat(),
            "source": random.choice(alert_type["sources"]),
            "status": random.choice(["Nouveau", "En cours", "Surveillance"]),
            "confidence": round(random.uniform(0.5, 1.0), 2)
        }
        
        # Fusionner avec les données spécifiques
        alert.update(specific_data)
        
        return alert
    
    def send_alert(self, alert=None):
        """Envoie une alerte à Kafka"""
        if alert is None:
            alert = self.generate_alert()
        
        try:
            future = self.producer.send(self.topic, value=alert)
            # Attendre la confirmation
            result = future.get(timeout=10)
            
            logger.info(f"📤 Alerte envoyée: {alert['alert_id']} - {alert['type']} ({alert['region']})")
            return {
                "success": True,
                "alert_id": alert['alert_id'],
                "partition": result.partition,
                "offset": result.offset
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur envoi alerte: {e}")
            return {
                "success": False,
                "error": str(e),
                "alert_id": alert.get('alert_id', 'unknown')
            }
    
    def send_bulk_alerts(self, count=10, interval=2):
        """Envoie plusieurs alertes avec un intervalle"""
        logger.info(f"🚀 Envoi de {count} alertes avec intervalle de {interval}s")
        
        results = []
        for i in range(count):
            result = self.send_alert()
            results.append(result)
            
            if i < count - 1:  # Pas de sleep après la dernière
                time.sleep(interval)
        
        return results
    
    def close(self):
        """Ferme le producteur"""
        self.producer.close()
        logger.info("👋 Producteur Kafka fermé")

def main():
    """Fonction principale - Mode interactif"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Producteur d\'alertes climatiques Kafka')
    parser.add_argument('--mode', choices=['single', 'bulk', 'continuous'], 
                       default='single', help='Mode d\'envoi')
    parser.add_argument('--count', type=int, default=10, help='Nombre d\'alertes (bulk mode)')
    parser.add_argument('--interval', type=int, default=2, help='Intervalle en secondes')
    
    args = parser.parse_args()
    
    producer = ClimateAlertProducer()
    
    try:
        if args.mode == 'single':
            result = producer.send_alert()
            print(f"Résultat: {result}")
            
        elif args.mode == 'bulk':
            results = producer.send_bulk_alerts(args.count, args.interval)
            successes = sum(1 for r in results if r.get('success', False))
            print(f"✅ {successes}/{len(results)} alertes envoyées avec succès")
            
        elif args.mode == 'continuous':
            print("🔁 Mode continu - Ctrl+C pour arrêter")
            alert_count = 0
            while True:
                result = producer.send_alert()
                alert_count += 1
                print(f"[{alert_count}] {result}")
                time.sleep(args.interval)
                
    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur")
    finally:
        producer.close()

if __name__ == "__main__":
    main()