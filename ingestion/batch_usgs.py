#!/usr/bin/env python3
"""
Script d'ingestion USGS simulé pour Streamlit
"""
import pandas as pd
import numpy as np
from datetime import datetime
import json
import sys

def main():
    print("🚀 Démarrage ingestion USGS simulée...")
    
    # Générer des données simulées
    df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=850, freq='H'),
        'magnitude': np.random.uniform(2, 8, 850),
        'latitude': np.random.uniform(30, 50, 850),
        'longitude': np.random.uniform(-130, -60, 850),
        'depth': np.random.uniform(1, 100, 850)
    })
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    result = {
        'success': True,
        'records': len(df),
        'hdfs_path': f'/hadoop-climate-risk/raw/usgs/earthquakes_{timestamp}.parquet',
        'timestamp': timestamp,
        'message': f'✅ {len(df)} séismes USGS collectés'
    }
    
    # Écrire le résultat au format JSON pour Streamlit
    print(json.dumps(result))
    return 0

if __name__ == "__main__":
    sys.exit(main())