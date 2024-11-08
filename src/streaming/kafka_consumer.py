# Kafka consumer to pull data from Kafka

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from config.kafka_config import KAFKA_BROKER_URL, TOPIC_NAME_FL_DASHBOARD, TOPIC_NAME_SCHOOL_CODELIST
from config.spark_config import APP_NAME, MASTER, BATCH_DURATION
import json

def start_spark_streaming():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(MASTER) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
        .option("subscribe", f"{TOPIC_NAME_FL_DASHBOARD},{TOPIC_NAME_SCHOOL_CODELIST}") \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert Kafka's binary data to string
    df_kafka = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Define schema or processing functions as needed for incoming data
    # This example assumes JSON strings; adjust according to your data format
    schema = "your_schema_here"  # Define your schema here

    df_parsed = df_kafka.withColumn("value", from_json(col("value"), schema))

    # Write the parsed data to the console (for debugging)
    query = df_parsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Wait for the termination signal
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_streaming()