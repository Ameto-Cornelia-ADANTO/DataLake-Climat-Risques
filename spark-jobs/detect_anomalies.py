#!/usr/bin/env python3
"""
Job Spark pour la détection d'anomalies dans les données climatiques et sismiques
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, abs, lit
from pyspark.sql.window import Window
import logging
from datetime import datetime
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark():
    """Initialise la session Spark"""
    return SparkSession.builder \
        .appName("Anomaly_Detection_Job") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def detect_temperature_anomalies(spark):
    """Détecte les anomalies de température"""
    logger.info("🌡️ Détection anomalies température...")
    
    # Lire les données NOAA nettoyées
    try:
        df_noaa = spark.read.parquet("hdfs://namenode:9000/hadoop-climate-risk/silver/noaa_cleaned_*")
        logger.info(f"✅ Données NOAA chargées: {df_noaa.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée NOAA trouvée")
        return None
    
    # Calculer les statistiques par station
    window_spec = Window.partitionBy("station_id").orderBy("date").rowsBetween(-30, 0)
    
    df_anomalies = df_noaa.withColumn("avg_temp_30d", avg("avg_temp").over(window_spec)) \
        .withColumn("std_temp_30d", stddev("avg_temp").over(window_spec)) \
        .withColumn("z_score", 
            when(col("std_temp_30d") != 0, 
                abs((col("avg_temp") - col("avg_temp_30d")) / col("std_temp_30d"))
            ).otherwise(0)
        ) \
        .withColumn("is_anomaly", col("z_score") > 3) \
        .withColumn("anomaly_type",
            when((col("avg_temp") - col("avg_temp_30d")) > 0, "High")
            .otherwise("Low")
        )
    
    # Filtrer seulement les anomalies
    df_detected = df_anomalies.filter(col("is_anomaly") == True) \
        .select(
            "date", "station_id", "avg_temp", "avg_temp_30d",
            "z_score", "anomaly_type", "latitude", "longitude"
        )
    
    logger.info(f"✅ {df_detected.count()} anomalies température détectées")
    return df_detected

def detect_precipitation_anomalies(spark):
    """Détecte les anomalies de précipitations"""
    logger.info("🌧️ Détection anomalies précipitations...")
    
    try:
        df_noaa = spark.read.parquet("hdfs://namenode:9000/hadoop-climate-risk/silver/noaa_cleaned_*")
    except:
        return None
    
    # Anomalies de précipitations (valeur absolue > 50mm ou 0mm pendant 10 jours)
    window_spec = Window.partitionBy("station_id").orderBy("date").rowsBetween(-10, 0)
    
    df_anomalies = df_noaa.withColumn("avg_precip_10d", avg("precipitation").over(window_spec)) \
        .withColumn("is_dry_spell", 
            when((col("precipitation") == 0) & (col("avg_precip_10d") == 0), True)
            .otherwise(False)
        ) \
        .withColumn("is_flood_risk", col("precipitation") > 50) \
        .filter((col("is_dry_spell") == True) | (col("is_flood_risk") == True)) \
        .withColumn("precip_anomaly_type",
            when(col("is_dry_spell") == True, "Sécheresse")
            .when(col("is_flood_risk") == True, "Risque inondation")
            .otherwise("Normal")
        )
    
    df_detected = df_anomalies.select(
        "date", "station_id", "precipitation", "avg_precip_10d",
        "precip_anomaly_type", "latitude", "longitude"
    )
    
    logger.info(f"✅ {df_detected.count()} anomalies précipitations détectées")
    return df_detected

def detect_seismic_anomalies(spark):
    """Détecte les anomalies sismiques"""
    logger.info("🌋 Détection anomalies sismiques...")
    
    try:
        df_usgs = spark.read.parquet("hdfs://namenode:9000/hadoop-climate-risk/silver/usgs_cleaned_*")
        logger.info(f"✅ Données USGS chargées: {df_usgs.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée USGS trouvée")
        return None
    
    # Détection de clusters sismiques
    from pyspark.sql.functions import count, avg as spark_avg
    
    # Agrégation par région et jour
    df_region = df_usgs.groupBy(
        "year", "month", "day",
        when(col("latitude").between(30, 50) & col("longitude").between(-130, -60), "USA_West")
        .when(col("latitude").between(50, 70) & col("longitude").between(-180, -130), "Alaska")
        .when(col("latitude").between(18, 30) & col("longitude").between(-160, -154), "Hawaii")
        .otherwise("Autre").alias("region")
    ).agg(
        count("*").alias("quake_count"),
        spark_avg("magnitude").alias("avg_magnitude"),
        spark_avg("depth").alias("avg_depth")
    )
    
    # Détection d'anomalies (plus de 10 séismes par jour dans une région)
    window_spec = Window.partitionBy("region").orderBy("year", "month", "day").rowsBetween(-7, 0)
    
    df_anomalies = df_region.withColumn("avg_quakes_7d", 
        spark_avg("quake_count").over(window_spec)) \
        .withColumn("is_seismic_swarm",
            when(col("quake_count") > 10, True)
            .when(col("quake_count") > (col("avg_quakes_7d") * 3), True)
            .otherwise(False)
        ) \
        .filter(col("is_seismic_swarm") == True)
    
    logger.info(f"✅ {df_anomalies.count()} anomalies sismiques détectées")
    return df_anomalies

def save_anomalies(spark, temp_anomalies, precip_anomalies, seismic_anomalies):
    """Sauvegarde les anomalies détectées"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gold_path = "hdfs://namenode:9000/hadoop-climate-risk/gold/anomalies"
    
    # Sauvegarder anomalies température
    if temp_anomalies is not None and temp_anomalies.count() > 0:
        temp_path = f"{gold_path}/temperature_{timestamp}"
        temp_anomalies.write \
            .mode("overwrite") \
            .parquet(temp_path)
        logger.info(f"💾 Anomalies température sauvegardées: {temp_path}")
    
    # Sauvegarder anomalies précipitations
    if precip_anomalies is not None and precip_anomalies.count() > 0:
        precip_path = f"{gold_path}/precipitation_{timestamp}"
        precip_anomalies.write \
            .mode("overwrite") \
            .parquet(precip_path)
        logger.info(f"💾 Anomalies précipitations sauvegardées: {precip_path}")
    
    # Sauvegarder anomalies sismiques
    if seismic_anomalies is not None and seismic_anomalies.count() > 0:
        seismic_path = f"{gold_path}/seismic_{timestamp}"
        seismic_anomalies.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(seismic_path)
        logger.info(f"💾 Anomalies sismiques sauvegardées: {seismic_path}")

def main():
    """Fonction principale"""
    logger.info("🚀 Démarrage job Anomaly Detection")
    
    # Initialiser Spark
    spark = init_spark()
    
    try:
        # Détecter les anomalies
        temp_anomalies = detect_temperature_anomalies(spark)
        precip_anomalies = detect_precipitation_anomalies(spark)
        seismic_anomalies = detect_seismic_anomalies(spark)
        
        # Sauvegarder
        save_anomalies(spark, temp_anomalies, precip_anomalies, seismic_anomalies)
        
        # Résumé
        logger.info("=" * 50)
        logger.info("📋 RÉSUMÉ ANOMALY DETECTION")
        if temp_anomalies:
            logger.info(f"   Anomalies température: {temp_anomalies.count()}")
        if precip_anomalies:
            logger.info(f"   Anomalies précipitations: {precip_anomalies.count()}")
        if seismic_anomalies:
            logger.info(f"   Anomalies sismiques: {seismic_anomalies.count()}")
        logger.info("✅ Job Anomaly Detection terminé avec succès")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Erreur dans le job de détection d'anomalies: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()