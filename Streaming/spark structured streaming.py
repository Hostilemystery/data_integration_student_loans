from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main():
    # Configuration de SparkSession avec le connecteur Kafka
    spark = SparkSession.builder \
        .appName("KafkaToHDFS") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Définition du schéma des messages en JSON
    message_schema = StructType([
        StructField("SchoolCode", StringType(), True),
        StructField("SchoolName", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("StateCode", StringType(), True),
        StructField("ZipCode", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    # Lecture depuis Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "excel_data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Extraction des valeurs du message
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")

    # Convertion de la chaîne JSON en colonnes structurées
    json_df = value_df.select(from_json(col("json_string"), message_schema).alias("data")).select("data.*")

    # Convertion du timestamp en format timestamp
    processed_df = json_df.withColumn("event_time", (col("timestamp")).cast(TimestampType()))

    hdfs_output_path = "hdfs://localhost:9080/user/anthonycormeaux/data/dataframes"

    # Écrire le flux de données dans HDFS en format Parquet, chaque batch sera un nouveau parquet
    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", hdfs_output_path) \
        .option("checkpointLocation", "hdfs://localhost:9080/user/anthonycormeaux/data/process") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    print("Spark Structured Streaming est en cours d'exécution. Appuyez sur Ctrl+C pour arrêter.")
    query.awaitTermination()

if __name__ == "__main__":
    main()
