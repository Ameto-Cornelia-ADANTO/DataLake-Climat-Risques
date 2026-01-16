#!/usr/bin/env python3
"""
Script d'ingestion NOAA simulé pour Streamlit
"""
import pandas as pd
import numpy as np
from datetime import datetime
import json
import sys

def main():
    print("🚀 Démarrage ingestion NOAA simulée...")
    
    # Générer des données simulées
    dates = pd.date_range('2024-01-01', periods=1250)
    df = pd.DataFrame({
        'date': dates,
        'station_id': np.random.choice(['NYC001', 'LAX002', 'CHI003', 'MIA004'], 1250),
        'temperature': np.random.normal(15, 5, 1250),
        'humidity': np.random.uniform(30, 90, 1250),
        'precipitation': np.random.exponential(2, 1250)
    })
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    result = {
        'success': True,
        'records': len(df),
        'hdfs_path': f'/hadoop-climate-risk/raw/noaa/noaa_{timestamp}.parquet',
        'timestamp': timestamp,
        'message': f'✅ {len(df)} enregistrements NOAA ingérés'
    }
    
    # Écrire le résultat au format JSON pour Streamlit
    print(json.dumps(result))
    return 0

if __name__ == "__main__":
    sys.exit(main())