#!/usr/bin/env python3
"""
Job Spark pour calculer les tendances à long terme
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, sum, count, when
from pyspark.sql.functions import lag, lead, percent_rank, row_number
from pyspark.sql.window import Window
import logging
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark():
    """Initialise la session Spark"""
    return SparkSession.builder \
        .appName("Trend_Calculation_Job") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

def calculate_climate_trends(spark):
    """Calcule les tendances climatiques"""
    logger.info("📈 Calcul tendances climatiques...")
    
    try:
        df_noaa = spark.read.parquet("hdfs://namenode:9000/hadoop-climate-risk/silver/noaa_cleaned_*")
        logger.info(f"✅ Données NOAA chargées: {df_noaa.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée NOAA trouvée")
        return None
    
    # Agrégation mensuelle
    df_monthly = df_noaa.groupBy("year", "month").agg(
        avg("avg_temp").alias("avg_monthly_temp"),
        avg("max_temp").alias("avg_max_temp"),
        avg("min_temp").alias("avg_min_temp"),
        sum("precipitation").alias("total_precipitation"),
        sum("snow").alias("total_snow"),
        avg("avg_wind_speed").alias("avg_wind_speed"),
        count("*").alias("measurement_count")
    ).orderBy("year", "month")
    
    # Calcul des tendances avec fenêtre glissante
    window_spec = Window.orderBy("year", "month")
    
    df_trends = df_monthly \
        .withColumn("temp_prev_month", lag("avg_monthly_temp", 1).over(window_spec)) \
        .withColumn("temp_next_month", lead("avg_monthly_temp", 1).over(window_spec)) \
        .withColumn("temp_trend",
            when(col("avg_monthly_temp") > col("temp_prev_month"), "↑ Hausse")
            .when(col("avg_monthly_temp") < col("temp_prev_month"), "↓ Baisse")
            .otherwise("→ Stable")
        ) \
        .withColumn("precip_prev_month", lag("total_precipitation", 1).over(window_spec)) \
        .withColumn("precip_trend",
            when(col("total_precipitation") > col("precip_prev_month") * 1.5, "↑ Forte hausse")
            .when(col("total_precipitation") > col("precip_prev_month"), "↑ Hausse")
            .when(col("total_precipitation") < col("precip_prev_month") * 0.5, "↓ Forte baisse")
            .when(col("total_precipitation") < col("precip_prev_month"), "↓ Baisse")
            .otherwise("→ Stable")
        )
    
    # Calcul des moyennes annuelles
    df_yearly = df_monthly.groupBy("year").agg(
        avg("avg_monthly_temp").alias("avg_yearly_temp"),
        sum("total_precipitation").alias("total_yearly_precipitation"),
        avg("avg_wind_speed").alias("avg_yearly_wind_speed")
    ).orderBy("year")
    
    # Calcul du réchauffement global (tendance linéaire simplifiée)
    if df_yearly.count() > 1:
        # Convertir en Pandas pour calcul simple
        pdf_yearly = df_yearly.orderBy("year").toPandas()
        
        # Régression linéaire simple
        import numpy as np
        years = pdf_yearly["year"].values.astype(float)
        temps = pdf_yearly["avg_yearly_temp"].values
        
        if len(years) > 1:
            coeff = np.polyfit(years, temps, 1)
            warming_per_decade = coeff[0] * 10  # °C par décennie
            
            logger.info(f"📊 Réchauffement estimé: {warming_per_decade:.3f}°C par décennie")
            
            # Ajouter au DataFrame
            warming_df = spark.createDataFrame([{
                "metric": "global_warming_rate",
                "value": float(warming_per_decade),
                "unit": "°C/décennie",
                "calculation_date": datetime.now().strftime("%Y-%m-%d")
            }])
        else:
            warming_df = None
    else:
        warming_df = None
    
    logger.info(f"✅ Tendances climatiques calculées: {df_trends.count()} mois")
    return df_trends, df_yearly, warming_df

def calculate_seismic_trends(spark):
    """Calcule les tendances sismiques"""
    logger.info("📈 Calcul tendances sismiques...")
    
    try:
        df_usgs = spark.read.parquet("hdfs://namenode:9000/hadoop-climate-risk/silver/usgs_cleaned_*")
        logger.info(f"✅ Données USGS chargées: {df_usgs.count()} lignes")
    except:
        logger.warning("⚠️ Aucune donnée USGS trouvée")
        return None, None
    
    # Agrégation mensuelle
    df_monthly = df_usgs.groupBy("year", "month").agg(
        count("*").alias("earthquake_count"),
        avg("magnitude").alias("avg_magnitude"),
        avg("depth").alias("avg_depth"),
        count(when(col("severity") >= 3, True)).alias("significant_quakes"),
        count(when(col("magnitude") >= 6.0, True)).alias("major_quakes")
    ).orderBy("year", "month")
    
    # Calcul des tendances
    window_spec = Window.orderBy("year", "month")
    
    df_trends = df_monthly \
        .withColumn("quake_prev_month", lag("earthquake_count", 1).over(window_spec)) \
        .withColumn("quake_trend",
            when(col("earthquake_count") > col("quake_prev_month") * 1.5, "↑ Forte hausse")
            .when(col("earthquake_count") > col("quake_prev_month"), "↑ Hausse")
            .when(col("earthquake_count") < col("quake_prev_month") * 0.5, "↓ Forte baisse")
            .when(col("earthquake_count") < col("quake_prev_month"), "↓ Baisse")
            .otherwise("→ Stable")
        ) \
        .withColumn("mag_prev_month", lag("avg_magnitude", 1).over(window_spec)) \
        .withColumn("mag_trend",
            when(col("avg_magnitude") > col("mag_prev_month") + 0.5, "↑ Hausse significative")
            .when(col("avg_magnitude") > col("mag_prev_month"), "↑ Légère hausse")
            .when(col("avg_magnitude") < col("mag_prev_month") - 0.5, "↓ Baisse significative")
            .when(col("avg_magnitude") < col("mag_prev_month"), "↓ Légère baisse")
            .otherwise("→ Stable")
        )
    
    # Calcul des statistiques annuelles
    df_yearly = df_monthly.groupBy("year").agg(
        sum("earthquake_count").alias("total_earthquakes"),
        avg("avg_magnitude").alias("avg_yearly_magnitude"),
        sum("significant_quakes").alias("total_significant_quakes"),
        sum("major_quakes").alias("total_major_quakes")
    ).orderBy("year")
    
    logger.info(f"✅ Tendances sismiques calculées: {df_trends.count()} mois")
    return df_trends, df_yearly

def save_trends(spark, climate_monthly, climate_yearly, warming_rate, seismic_monthly, seismic_yearly):
    """Sauvegarde toutes les tendances"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trends_path = "hdfs://namenode:9000/hadoop-climate-risk/gold/trends"
    
    # Sauvegarder tendances climatiques mensuelles
    if climate_monthly is not None:
        climate_monthly_path = f"{trends_path}/climate_monthly_{timestamp}"
        climate_monthly.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .parquet(climate_monthly_path)
        logger.info(f"💾 Tendances climatiques mensuelles: {climate_monthly_path}")
    
    # Sauvegarder tendances climatiques annuelles
    if climate_yearly is not None:
        climate_yearly_path = f"{trends_path}/climate_yearly_{timestamp}"
        climate_yearly.write \
            .mode("overwrite") \
            .parquet(climate_yearly_path)
        logger.info(f"💾 Tendances climatiques annuelles: {climate_yearly_path}")
    
    # Sauvegarder taux de réchauffement
    if warming_rate is not None:
        warming_path = f"{trends_path}/warming_rate_{timestamp}"
        warming_rate.write \
            .mode("overwrite") \
            .parquet(warming_path)
        logger.info(f"💾 Taux de réchauffement: {warming_path}")
    
    # Sauvegarder tendances sismiques mensuelles
    if seismic_monthly is not None:
        seismic_monthly_path = f"{trends_path}/seismic_monthly_{timestamp}"
        seismic_monthly.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .parquet(seismic_monthly_path)
        logger.info(f"💾 Tendances sismiques mensuelles: {seismic_monthly_path}")
    
    # Sauvegarder tendances sismiques annuelles
    if seismic_yearly is not None:
        seismic_yearly_path = f"{trends_path}/seismic_yearly_{timestamp}"
        seismic_yearly.write \
            .mode("overwrite") \
            .parquet(seismic_yearly_path)
        logger.info(f"💾 Tendances sismiques annuelles: {seismic_yearly_path}")

def main():
    """Fonction principale"""
    logger.info("🚀 Démarrage job Trend Calculation")
    
    # Initialiser Spark
    spark = init_spark()
    
    try:
        # Calculer les tendances
        climate_monthly, climate_yearly, warming_rate = calculate_climate_trends(spark)
        seismic_monthly, seismic_yearly = calculate_seismic_trends(spark)
        
        # Sauvegarder
        save_trends(spark, climate_monthly, climate_yearly, warming_rate, 
                   seismic_monthly, seismic_yearly)
        
        # Résumé
        logger.info("=" * 50)
        logger.info("📋 RÉSUMÉ TREND CALCULATION")
        if climate_monthly:
            logger.info(f"   Tendances climatiques: {climate_monthly.count()} mois")
        if climate_yearly:
            logger.info(f"   Statistiques annuelles climat: {climate_yearly.count()} années")
        if seismic_monthly:
            logger.info(f"   Tendances sismiques: {seismic_monthly.count()} mois")
        if seismic_yearly:
            logger.info(f"   Statistiques annuelles sismiques: {seismic_yearly.count()} années")
        if warming_rate:
            logger.info("   Taux de réchauffement calculé")
        logger.info("✅ Job Trend Calculation terminé avec succès")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Erreur dans le job de calcul de tendances: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()