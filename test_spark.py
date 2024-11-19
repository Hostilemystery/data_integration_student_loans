import os

# 1. Définir PYSPARK_SUBMIT_ARGS AVANT d'initialiser findspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

import findspark
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# 2. Créer une seule session Spark
spark = SparkSession.builder \
    .appName("SparkKafkaIntegration") \
    .master("local[*]") \
    .getOrCreate()

# 3. Afficher les versions de Spark et Scala
print("Version de Spark :", spark.version)
scala_version = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
print("Version de Scala :", scala_version)

# 4. Configurations Kafka
kafka_input_config = {
    "kafka.bootstrap.servers": "localhost:9092",  # Assurez-vous que Kafka est accessible via localhost:9092
    "subscribe": "excel_data",                     # Topic d'entrée
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}
kafka_output_config = {
    "kafka.bootstrap.servers": "localhost:9092",
    "topic": "excel_data_output",                  # Topic de sortie distinct
    "checkpointLocation": "./checkpoints/excel_data_output"  # Répertoire dédié pour les checkpoints
}

# 5. Créer le répertoire de checkpoints si nécessaire
checkpoint_dir = "./checkpoints/excel_data_output"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# 6. Schéma d'entrée
df_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("merchant_id", StringType(), True)
])

# 7. Lire le flux depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_input_config) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), df_schema).alias("json_data")) \
    .select("json_data.*")

# 8. Filtrer les transactions éligibles
df_filtered = df.filter(
    (F.col("amount") >= 15) &
    (F.dayofweek(F.col("transaction_timestamp")) == 6) &  # 6 = Vendredi (selon Spark, 1 = Dimanche)
    (F.col("merchant_id") == "MerchantX")
).withColumn("cashback", F.col("amount").cast("double") * 0.15) \
 .select(
    F.col("customer_id"),
    F.col("amount"),
    F.col("transaction_timestamp"),
    F.col("merchant_id"),
    F.col("cashback")
 )

# 9. Préparer la sortie pour Kafka
output_df = df_filtered.select(F.to_json(F.struct(*df_filtered.columns)).alias("value"))

# 10. Écrire le flux vers Kafka
write = output_df \
    .writeStream \
    .format("kafka") \
    .options(**kafka_output_config) \
    .outputMode("append") \
    .start()

write.awaitTermination()
