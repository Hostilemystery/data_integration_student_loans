from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# 1. Initialiser la session Spark
spark = SparkSession.builder \
    .appName("KafkaToHDFSUpdater") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définir les schémas
csv_schema = StructType([
    StructField("OPE ID", StringType(), True),
    StructField("SchoolName", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zip Code", StringType(), True),
    StructField("School Type", StringType(), True),
    StructField("ffel_subsidized_recipients", IntegerType(), True),
    # Ajoutez les autres champs selon votre CSV
    StructField("Timestamp", TimestampType(), True)
])

kafka_schema = StructType([
    StructField("SchoolCode", StringType(), True),
    StructField("SchoolName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("StateCode", StringType(), True),
    StructField("ZipCode", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("timestamp", DoubleType(), True)  # Timestamp en secondes depuis epoch
])

# 3. Lire le fichier CSV initial et le mettre en cache
csv_path = "hdfs://localhost:9080/user/anthonycormeaux/data/combined/combined_data.csv"

csv_df = spark.read \
    .option("header", "true") \
    .schema(csv_schema) \
    .csv(csv_path) \
    .cache()

# 4. Lire les messages du topic Kafka
kafka_bootstrap_servers = "votre.kafka.broker:9092"
kafka_topic = "votre_topic_kafka"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parser les messages Kafka au format JSON
parsed_kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), kafka_schema).alias("data")) \
    .select("data.*") \
    .withColumn("message_timestamp", (col("timestamp") / 1000).cast(TimestampType()))  # Convertir en Timestamp

# 6. Fonction de mise à jour du CSV
def update_csv(batch_df, batch_id):
    global csv_df
    # Joindre le batch Kafka avec le CSV en mémoire
    updated_df = csv_df.alias("csv") \
        .join(batch_df.alias("kafka"), on="SchoolName", how="left") \
        .select(
            when(col("kafka.SchoolCode").isNotNull() & (col("kafka.message_timestamp") > col("csv.Timestamp")),
                 col("kafka.SchoolCode")).otherwise(col("csv.OPE ID")).alias("OPE ID"),
            col("csv.SchoolName"),
            when(col("kafka.StateCode").isNotNull() & (col("kafka.message_timestamp") > col("csv.Timestamp")),
                 col("kafka.StateCode")).otherwise(col("csv.State")).alias("State"),
            when(col("kafka.ZipCode").isNotNull() & (col("kafka.message_timestamp") > col("csv.Timestamp")),
                 col("kafka.ZipCode").cast(StringType())).otherwise(col("csv.Zip Code")).alias("Zip Code"),
            col("csv.School Type"),
            # Inclure les autres colonnes non modifiées
            col("csv.ffel_subsidized_recipients"),
            # ... autres colonnes ...
            when(col("kafka.message_timestamp").isNotNull() & (col("kafka.message_timestamp") > col("csv.Timestamp")),
                 col("kafka.message_timestamp")).otherwise(col("csv.Timestamp")).alias("Timestamp")
        )
    
    # Mettre à jour le DataFrame en mémoire
    csv_df = updated_df.cache()
    
    # Écrire le CSV mis à jour sur HDFS
    updated_df.write.mode("overwrite").option("header", "true").csv(csv_path)

# 7. Appliquer la fonction de mise à jour à chaque micro-batch
query = parsed_kafka_df.writeStream \
    .foreachBatch(update_csv) \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .start()

query.awaitTermination()