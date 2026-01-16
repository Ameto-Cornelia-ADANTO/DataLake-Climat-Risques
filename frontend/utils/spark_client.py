import subprocess
import os
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkJobClient:
    """Client pour lancer et monitorer des jobs Spark"""
    
    def __init__(self, spark_master="spark://spark-master:7077"):
        self.spark_master = spark_master
        self.job_history = []
        
    def submit_job(self, app_path, job_name=None, spark_args=None):
        """
        Soumet un job Spark
        
        Args:
            app_path: Chemin vers le script Spark (.py)
            job_name: Nom du job
            spark_args: Arguments supplémentaires pour spark-submit
            
        Returns:
            dict: Résultats de l'exécution
        """
        if not os.path.exists(app_path):
            return {
                "success": False,
                "error": f"Fichier non trouvé: {app_path}",
                "job_id": None
            }
        
        if job_name is None:
            job_name = os.path.basename(app_path).replace(".py", "")
        
        # Construction de la commande
        cmd = [
            "spark-submit",
            "--master", self.spark_master,
            "--name", f"Streamlit_{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "--conf", "spark.sql.shuffle.partitions=2",
            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.executor.memory=1g",
        ]
        
        # Ajout des arguments spécifiques
        if spark_args:
            cmd.extend(spark_args)
        
        cmd.append(app_path)
        
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🚀 Lancement job Spark: {job_name}")
        logger.info(f"   Command: {' '.join(cmd)}")
        
        try:
            # Exécution asynchrone
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Attente avec timeout
            stdout, stderr = process.communicate(timeout=300)  # 5 minutes timeout
            
            job_result = {
                "job_id": job_id,
                "job_name": job_name,
                "app_path": app_path,
                "success": process.returncode == 0,
                "return_code": process.returncode,
                "stdout": stdout,
                "stderr": stderr,
                "timestamp": datetime.now().isoformat(),
                "duration": None  # À calculer si nécessaire
            }
            
            if job_result["success"]:
                logger.info(f"✅ Job {job_name} terminé avec succès")
            else:
                logger.error(f"❌ Job {job_name} échoué: {process.returncode}")
            
            # Sauvegarde dans l'historique
            self.job_history.append(job_result)
            
            return job_result
            
        except subprocess.TimeoutExpired:
            logger.error(f"⏱️ Job {job_name} timeout (5 minutes)")
            return {
                "job_id": job_id,
                "job_name": job_name,
                "success": False,
                "error": "Timeout - Le job a pris trop de temps",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution job: {e}")
            return {
                "job_id": job_id,
                "job_name": job_name,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_job_status(self, spark_ui_url="http://spark-master:8080"):
        """
        Récupère le statut des jobs depuis l'UI Spark
        
        Note: Cette fonction nécessite des requêtes HTTP vers l'UI Spark
        """
        import requests
        
        try:
            response = requests.get(f"{spark_ui_url}/api/v1/applications", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": f"Statut {response.status_code}"}
        except:
            return {"error": "Impossible de se connecter à l'UI Spark"}
    
    def get_job_history(self, limit=10):
        """Retourne l'historique des jobs lancés"""
        return sorted(self.job_history, 
                     key=lambda x: x["timestamp"], 
                     reverse=True)[:limit]
    
    def run_etl_cleaning(self):
        """Lance le job de nettoyage ETL"""
        script_path = "spark-jobs/etl_cleaning.py"
        return self.submit_job(script_path, "ETL_Cleaning")
    
    def run_daily_aggregation(self):
        """Lance le job d'agrégation quotidienne"""
        script_path = "spark-jobs/daily_aggregation.py"
        return self.submit_job(script_path, "Daily_Aggregation")
    
    def run_anomaly_detection(self):
        """Lance le job de détection d'anomalies"""
        script_path = "spark-jobs/detect_anomalies.py"
        return self.submit_job(script_path, "Anomaly_Detection")

# Instance globale
spark_client = SparkJobClient()