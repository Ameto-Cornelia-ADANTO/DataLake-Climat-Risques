#!/usr/bin/env python3
"""
Job Spark pour l'agrégation quotidienne des données
Crée des vues agrégées pour le dashboard
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, sum, date_format
from pyspark.sql.functions import when, year, month, dayofmonth
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark():
    """Initialise la session Spark"""
    return SparkSession.builder \
        .appName("Daily_Aggregation_Job") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def aggregate_noaa_daily(spark):
    """Agrège les données NOAA par jour et station"""
    logger.info("📊 Agrégation quotidienne NOAA...")
    
    # Lire les données nettoyées
    silver_path = "hdfs://namenode:9000/hadoop-climate-risk/silver/"
    try:
        df_noaa = spark.read.parquet(f"{silver_path}/noaa_cleaned_*")
        logger.info(f"✅ Données NOAA chargées: {df_noaa.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée NOAA nettoyée trouvée")
        return None
    
    # Agrégation par jour et station
    df_daily = df_noaa.groupBy("date", "station_id", "year", "month", "day").agg(
        avg("max_temp").alias("avg_max_temp"),
        avg("min_temp").alias("avg_min_temp"),
        avg("avg_temp").alias("avg_daily_temp"),
        sum("precipitation").alias("total_precipitation"),
        sum("snow").alias("total_snow"),
        avg("avg_wind_speed").alias("avg_wind_speed"),
        count("*").alias("measurement_count")
    )
    
    # Ajouter des indicateurs
    df_daily = df_daily.withColumn(
        "precipitation_category",
        when(col("total_precipitation") == 0, "Sec")
        .when(col("total_precipitation") < 5, "Léger")
        .when(col("total_precipitation") < 20, "Modéré")
        .otherwise("Fort")
    )
    
    logger.info(f"✅ NOAA agrégé: {df_daily.count()} lignes journalières")
    return df_daily

def aggregate_usgs_daily(spark):
    """Agrège les données USGS par jour"""
    logger.info("📊 Agrégation quotidienne USGS...")
    
    # Lire les données nettoyées
    silver_path = "hdfs://namenode:9000/hadoop-climate-risk/silver/"
    try:
        df_usgs = spark.read.parquet(f"{silver_path}/usgs_cleaned_*")
        logger.info(f"✅ Données USGS chargées: {df_usgs.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée USGS nettoyée trouvée")
        return None
    
    # Agrégation par jour
    df_daily = df_usgs.groupBy(
        date_format(col("timestamp"), "yyyy-MM-dd").alias("date"),
        "year", "month"
    ).agg(
        count("*").alias("earthquake_count"),
        avg("magnitude").alias("avg_magnitude"),
        max("magnitude").alias("max_magnitude"),
        min("magnitude").alias("min_magnitude"),
        avg("depth").alias("avg_depth"),
        count(when(col("severity") >= 3, True)).alias("significant_quakes")
    )
    
    # Ajouter des indicateurs
    df_daily = df_daily.withColumn(
        "activity_level",
        when(col("earthquake_count") == 0, "Calme")
        .when(col("earthquake_count") < 5, "Faible")
        .when(col("earthquake_count") < 10, "Modéré")
        .otherwise("Élevé")
    )
    
    logger.info(f"✅ USGS agrégé: {df_daily.count()} jours d'activité sismique")
    return df_daily

def create_cross_aggregation(spark, df_noaa_daily, df_usgs_daily):
    """Crée une agrégation croisée NOAA-USGS"""
    if df_noaa_daily is None or df_usgs_daily is None:
        return None
    
    logger.info("🔄 Création agrégation croisée...")
    
    # Préparer les données pour la jointure
    noaa_prep = df_noaa_daily.groupBy("date").agg(
        avg("avg_daily_temp").alias("avg_temp_all_stations"),
        max("avg_max_temp").alias("max_temp_day"),
        min("avg_min_temp").alias("min_temp_day"),
        avg("total_precipitation").alias("avg_precipitation")
    )
    
    usgs_prep = df_usgs_daily.select(
        col("date"),
        col("earthquake_count"),
        col("avg_magnitude"),
        col("activity_level")
    )
    
    # Jointure
    df_cross = noaa_prep.join(usgs_prep, "date", "outer")
    
    # Remplir les valeurs manquantes
    df_cross = df_cross.fillna({
        "earthquake_count": 0,
        "avg_magnitude": 0,
        "activity_level": "Calme",
        "avg_temp_all_stations": 0,
        "max_temp_day": 0,
        "min_temp_day": 0,
        "avg_precipitation": 0
    })
    
    # Ajouter une colonne de corrélation
    df_cross = df_cross.withColumn(
        "weather_seismic_correlation",
        when((col("earthquake_count") > 5) & (col("avg_temp_all_stations") > 25), "Possible")
        .when((col("earthquake_count") > 0) & (col("max_temp_day") - col("min_temp_day") > 15), "À étudier")
        .otherwise("Non détectée")
    )
    
    logger.info(f"✅ Agrégation croisée: {df_cross.count()} jours")
    return df_cross

def save_aggregations(spark, df_noaa_daily, df_usgs_daily, df_cross):
    """Sauvegarde toutes les agrégations dans GOLD"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gold_path = "hdfs://namenode:9000/hadoop-climate-risk/gold"
    
    # Sauvegarder NOAA daily
    if df_noaa_daily is not None:
        noaa_gold = f"{gold_path}/noaa_daily_agg_{timestamp}"
        df_noaa_daily.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(noaa_gold)
        logger.info(f"💾 NOAA daily sauvegardé: {noaa_gold}")
    
    # Sauvegarder USGS daily
    if df_usgs_daily is not None:
        usgs_gold = f"{gold_path}/usgs_daily_agg_{timestamp}"
        df_usgs_daily.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(usgs_gold)
        logger.info(f"💾 USGS daily sauvegardé: {usgs_gold}")
    
    # Sauvegarder cross aggregation
    if df_cross is not None:
        cross_gold = f"{gold_path}/cross_daily_agg_{timestamp}"
        df_cross.write \
            .mode("overwrite") \
            .parquet(cross_gold)
        logger.info(f"💾 Cross aggregation sauvegardé: {cross_gold}")

def main():
    """Fonction principale"""
    logger.info("🚀 Démarrage job Daily Aggregation")
    
    # Initialiser Spark
    spark = init_spark()
    
    try:
        # Créer les agrégations
        df_noaa_daily = aggregate_noaa_daily(spark)
        df_usgs_daily = aggregate_usgs_daily(spark)
        df_cross = create_cross_aggregation(spark, df_noaa_daily, df_usgs_daily)
        
        # Sauvegarder
        save_aggregations(spark, df_noaa_daily, df_usgs_daily, df_cross)
        
        # Résumé
        logger.info("=" * 50)
        logger.info("📋 RÉSUMÉ DAILY AGGREGATION")
        if df_noaa_daily:
            logger.info(f"   NOAA daily: {df_noaa_daily.count()} lignes")
        if df_usgs_daily:
            logger.info(f"   USGS daily: {df_usgs_daily.count()} lignes")
        if df_cross:
            logger.info(f"   Cross aggregation: {df_cross.count()} lignes")
        logger.info("✅ Job Daily Aggregation terminé avec succès")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Erreur dans le job d'agrégation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()