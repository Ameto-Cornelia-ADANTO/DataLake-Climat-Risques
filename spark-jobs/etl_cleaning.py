#!/usr/bin/env python3
"""
Job Spark pour le nettoyage ETL des données NOAA et USGS
Transforme les données RAW en données SILVER nettoyées
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, trim, upper, regexp_replace
from pyspark.sql.functions import date_format, to_date, year, month, dayofmonth
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark():
    """Initialise la session Spark"""
    return SparkSession.builder \
        .appName("ETL_Cleaning_Job") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def clean_noaa_data(spark):
    """Nettoie les données NOAA"""
    logger.info("🧹 Nettoyage des données NOAA...")
    
    # Lire les données RAW
    raw_path = "hdfs://namenode:9000/hadoop-climate-risk/raw/noaa/"
    try:
        df_noaa = spark.read.parquet(raw_path)
        logger.info(f"✅ Données NOAA chargées: {df_noaa.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée NOAA trouvée dans RAW")
        return None
    
    # Nettoyage des colonnes
    df_clean = df_noaa.select(
        col("DATE").alias("date"),
        col("STATION").alias("station_id"),
        when(col("TMAX").isNull() | isnan(col("TMAX")), 0.0).otherwise(col("TMAX")).alias("max_temp"),
        when(col("TMIN").isNull() | isnan(col("TMIN")), 0.0).otherwise(col("TMIN")).alias("min_temp"),
        when(col("PRCP").isNull() | isnan(col("PRCP")), 0.0).otherwise(col("PRCP")).alias("precipitation"),
        when(col("SNOW").isNull() | isnan(col("SNOW")), 0.0).otherwise(col("SNOW")).alias("snow"),
        when(col("AWND").isNull() | isnan(col("AWND")), 0.0).otherwise(col("AWND")).alias("avg_wind_speed"),
        col("LATITUDE").alias("latitude"),
        col("LONGITUDE").alias("longitude"),
        col("INGESTION_TIMESTAMP").alias("ingestion_timestamp")
    )
    
    # Filtrer les données aberrantes
    df_clean = df_clean.filter(
        (col("max_temp") >= -50) & (col("max_temp") <= 60) &
        (col("min_temp") >= -50) & (col("min_temp") <= 60) &
        (col("precipitation") >= 0) & (col("precipitation") <= 1000) &
        (col("snow") >= 0) & (col("snow") <= 500) &
        (col("avg_wind_speed") >= 0) & (col("avg_wind_speed") <= 200)
    )
    
    # Ajouter des colonnes dérivées
    df_clean = df_clean.withColumn("avg_temp", (col("max_temp") + col("min_temp")) / 2)
    df_clean = df_clean.withColumn("year", year(col("date")))
    df_clean = df_clean.withColumn("month", month(col("date")))
    df_clean = df_clean.withColumn("day", dayofmonth(col("date")))
    
    logger.info(f"✅ NOAA nettoyé: {df_clean.count()} lignes")
    return df_clean

def clean_usgs_data(spark):
    """Nettoie les données USGS"""
    logger.info("🧹 Nettoyage des données USGS...")
    
    # Lire les données RAW
    raw_path = "hdfs://namenode:9000/hadoop-climate-risk/raw/usgs/"
    try:
        df_usgs = spark.read.parquet(raw_path)
        logger.info(f"✅ Données USGS chargées: {df_usgs.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée USGS trouvée dans RAW")
        return None
    
    # Nettoyage des colonnes
    df_clean = df_usgs.select(
        col("earthquake_id").alias("quake_id"),
        when(col("magnitude").isNull() | isnan(col("magnitude")), 0.0).otherwise(col("magnitude")).alias("magnitude"),
        trim(col("place")).alias("location"),
        col("time").alias("timestamp"),
        col("longitude"),
        col("latitude"),
        when(col("depth").isNull() | isnan(col("depth")), 0.0).otherwise(col("depth")).alias("depth"),
        when(col("gap").isNull() | isnan(col("gap")), 0.0).otherwise(col("gap")).alias("gap"),
        when(col("rms").isNull() | isnan(col("rms")), 0.0).otherwise(col("rms")).alias("rms"),
        col("ingestion_timestamp"),
        col("year"),
        col("month"),
        col("severity")
    )
    
    # Filtrer les données aberrantes
    df_clean = df_clean.filter(
        (col("magnitude") >= 0) & (col("magnitude") <= 10) &
        (col("depth") >= 0) & (col("depth") <= 1000) &
        (col("latitude") >= -90) & (col("latitude") <= 90) &
        (col("longitude") >= -180) & (col("longitude") <= 180)
    )
    
    # Ajouter des colonnes dérivées
    df_clean = df_clean.withColumn(
        "intensity",
        when(col("magnitude") < 4.0, "Faible")
        .when(col("magnitude") < 6.0, "Modéré")
        .when(col("magnitude") < 7.0, "Fort")
        .otherwise("Majeur")
    )
    
    logger.info(f"✅ USGS nettoyé: {df_clean.count()} lignes")
    return df_clean

def save_clean_data(spark, df_noaa, df_usgs):
    """Sauvegarde les données nettoyées dans SILVER"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sauvegarder NOAA
    if df_noaa is not None:
        noaa_path = f"hdfs://namenode:9000/hadoop-climate-risk/silver/noaa_cleaned_{timestamp}"
        df_noaa.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(noaa_path)
        logger.info(f"💾 NOAA nettoyé sauvegardé: {noaa_path}")
    
    # Sauvegarder USGS
    if df_usgs is not None:
        usgs_path = f"hdfs://namenode:9000/hadoop-climate-risk/silver/usgs_cleaned_{timestamp}"
        df_usgs.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(usgs_path)
        logger.info(f"💾 USGS nettoyé sauvegardé: {usgs_path}")

def main():
    """Fonction principale"""
    logger.info("🚀 Démarrage job ETL Cleaning")
    
    # Initialiser Spark
    spark = init_spark()
    
    try:
        # Nettoyer les données
        df_noaa_clean = clean_noaa_data(spark)
        df_usgs_clean = clean_usgs_data(spark)
        
        # Sauvegarder
        save_clean_data(spark, df_noaa_clean, df_usgs_clean)
        
        # Résumé
        logger.info("=" * 50)
        logger.info("📋 RÉSUMÉ ETL CLEANING")
        if df_noaa_clean:
            logger.info(f"   NOAA: {df_noaa_clean.count()} lignes nettoyées")
        if df_usgs_clean:
            logger.info(f"   USGS: {df_usgs_clean.count()} séismes nettoyés")
        logger.info("✅ Job ETL terminé avec succès")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Erreur dans le job ETL: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()