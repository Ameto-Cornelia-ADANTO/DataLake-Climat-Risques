#!/usr/bin/env python3
"""
Consumer Spark Streaming pour traiter les alertes Kafka et les stocker dans HDFS
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Consomme les alertes Kafka et les écrit dans HDFS"""
    
    logger.info("🚀 Démarrage Spark Streaming Consumer")
    
    # Initialisation Spark avec support Kafka
    spark = SparkSession.builder \
        .appName("ClimateAlertsStreaming") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/alerts") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .getOrCreate()
    
    # Schéma des alertes JSON
    alert_schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("severity", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("status", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("magnitude", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("mag_type", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("duration_days", IntegerType(), True),
        StructField("humidity", DoubleType(), True)
    ])
    
    # Lire depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "climate-alerts") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parser le JSON
    df_parsed = df.select(
        from_json(col("value").cast("string"), alert_schema).alias("data")
    ).select("data.*")
    
    # Ajouter des colonnes de timestamp
    from pyspark.sql.functions import current_timestamp, date_format
    
    df_final = df_parsed \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("year", date_format(col("timestamp"), "yyyy")) \
        .withColumn("month", date_format(col("timestamp"), "MM")) \
        .withColumn("day", date_format(col("timestamp"), "dd")) \
        .withColumn("hour", date_format(col("timestamp"), "HH"))
    
    # Afficher dans la console (pour debug)
    console_query = df_final.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Écrire dans HDFS en Parquet
    hdfs_query = df_final.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/hadoop-climate-risk/alerts") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/alerts-hdfs") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ Streaming démarré - Écoute des alertes Kafka")
    
    # Attendre la terminaison
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()