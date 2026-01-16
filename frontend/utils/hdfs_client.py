import streamlit as st
import pandas as pd
import os
from datetime import datetime

class HDFSClient:
    """Client HDFS simulé pour la démonstration"""
    
    def __init__(self, host='namenode', port=9000):
        self.connected = True  # Toujours connecté en mode simulation
        self.host = host
        self.port = port
        
        # Structure simulée HDFS
        self.structure = {
            "/hadoop-climate-risk": {
                "raw": {
                    "noaa": ["noaa_20240114.parquet", "noaa_20240113.parquet", "noaa_20240112.parquet"],
                    "usgs": ["earthquakes_2024.parquet", "seismic_data.parquet", "usgs_latest.json"]
                },
                "silver": {
                    "cleaned": ["noaa_cleaned.parquet", "usgs_cleaned.parquet"],
                    "normalized": ["data_normalized.parquet"]
                },
                "gold": {
                    "aggregates": ["daily_aggregates.parquet", "monthly_trends.parquet", "weekly_report.parquet"],
                    "reports": ["climate_report.json", "seismic_analysis.json", "correlation_study.json"]
                },
                "alerts": {
                    "kafka": ["climate-alerts.parquet", "alerts_stream.parquet"],
                    "processed": ["alerts_processed.parquet", "anomalies_detected.parquet"]
                }
            }
        }
    
    def list_files(self, path="/hadoop-climate-risk"):
        """Liste les fichiers dans un chemin HDFS (simulé)"""
        try:
            parts = path.strip('/').split('/')
            current = self.structure
            
            for part in parts:
                if part and part in current:
                    current = current[part]
                else:
                    return []
            
            if isinstance(current, dict):
                return [f"{path}/{key}/" for key in current.keys()]
            else:
                return current
        except:
            return []
    
    def read_parquet_head(self, path, n_rows=5):
        """Lit un fichier Parquet (simulé)"""
        try:
            # Données simulées selon le type de fichier
            if "noaa" in path:
                return pd.DataFrame({
                    'date': pd.date_range('2024-01-01', periods=n_rows),
                    'temperature': [15.2, 16.5, 14.8, 17.3, 15.9],
                    'precipitation': [0.0, 2.5, 0.0, 5.2, 1.3],
                    'station_id': ['NYC001', 'LAX002', 'CHI003', 'MIA004', 'SEA005']
                })
            elif "usgs" in path or "earthquake" in path:
                return pd.DataFrame({
                    'timestamp': pd.date_range('2024-01-01', periods=n_rows, freq='H'),
                    'magnitude': [4.5, 3.2, 5.1, 2.8, 4.9],
                    'latitude': [34.05, 36.17, 37.77, 40.71, 47.61],
                    'longitude': [-118.25, -120.72, -122.42, -74.01, -122.33],
                    'depth': [10.2, 15.5, 8.7, 22.1, 12.4]
                })
            else:
                return pd.DataFrame({
                    'column1': ['data1', 'data2', 'data3', 'data4', 'data5'],
                    'column2': [1, 2, 3, 4, 5],
                    'timestamp': pd.date_range('2024-01-01', periods=n_rows)
                })
        except Exception as e:
            return pd.DataFrame({"Error": [str(e)], "File": [path]})
    
    def get_hdfs_structure(self):
        """Retourne la structure complète HDFS"""
        return self.structure
    
    def write_to_hdfs(self, local_path, hdfs_path):
        """Écrit un fichier vers HDFS (simulé)"""
        return True, f"✅ Fichier uploadé vers {hdfs_path} (simulé)"
    
    def get_stats(self):
        """Retourne des statistiques HDFS simulées"""
        return {
            "total_files": 42,
            "total_size_gb": 2.4,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "namenode": f"{self.host}:{self.port}",
            "status": "✅ Connecté (mode simulation)"
        }