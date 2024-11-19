import os

# 1. Définir PYSPARK_SUBMIT_ARGS AVANT d'importer findspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-core_2.12:2.4.0 pyspark-shell'

import findspark
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# 2. Créer une seule session Spark avec Delta Lake
spark = SparkSession.builder \
    .appName("KafkaToDeltaLake") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 3. Afficher les versions de Spark et Scala
print("Version de Spark :", spark.version)
scala_version = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
print("Version de Scala :", scala_version)

# 4. Chemin vers le fichier CSV sur HDFS
csv_path = "hdfs://localhost:9080/user/anthonycormeaux/data/combined/combined_data.csv"  # Assurez-vous que le port est correct

# 5. Lire le CSV
try:
    csv_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_path)
    print("Lecture du CSV réussie.")
except Exception as e:
    print(f"Erreur lors de la lecture du CSV: {e}")
    spark.stop()
    exit(1)

# 6. Nettoyer les noms de colonnes
def clean_column_names(df):
    for column in df.columns:
        new_column = column.replace(' ', '_').replace('.', '_')
        df = df.withColumnRenamed(column, new_column)
    return df

cleaned_csv_df = clean_column_names(csv_df)
print("Noms des colonnes nettoyés :", cleaned_csv_df.columns)

# 7. Chemin de stockage Delta Lake sur HDFS
delta_path = "hdfs://localhost:9080/user/anthonycormeaux/data/delta/combined_data_delta"  # Utilisez le port correct

# 8. Écrire le DataFrame en Delta Lake (à exécuter une seule fois)
# Commenter cette section après la première exécution pour éviter de réécrire la table Delta à chaque exécution
try:
    cleaned_csv_df.write.format("delta").mode("overwrite").save(delta_path)
    print("Conversion du CSV en Delta Lake terminée.")
except Exception as e:
    print(f"Erreur lors de l'écriture en Delta Lake: {e}")
    spark.stop()
    exit(1)

# 9. Définir le schéma des messages Kafka
message_schema = StructType([
    StructField("SchoolCode", StringType(), True),
    StructField("SchoolName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("StateCode", StringType(), True),
    StructField("ZipCode", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("timestamp", DoubleType(), True)  # Epoch seconds
])

# 10. Configurations Kafka
kafka_input_config = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "excel_data",
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

# 11. Lire le flux Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_config) \
    .load()

# 12. Parser les messages JSON
parsed_df = kafka_df.select(F.from_json(F.col("value").cast("string"), message_schema).alias("data")).select("data.*")

# 13. Convertir le timestamp de l'époque en format timestamp
parsed_df = parsed_df.withColumn("message_timestamp", F.from_unixtime(F.col("timestamp")).cast(TimestampType()))

# 14. Préparer les données pour le merge
updates_df = parsed_df.select(
    F.col("SchoolCode"),
    F.col("SchoolName"),
    F.col("Address"),
    F.col("City"),
    F.col("StateCode"),
    F.col("ZipCode"),
    F.col("Country"),
    F.col("message_timestamp").alias("Timestamp")
)

# 15. Fonction de fusion pour les upserts
def upsert_to_delta(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} est vide, rien à faire.")
        return
    
    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
    except Exception as e:
        print(f"Erreur lors de l'accès à la table Delta: {e}")
        return
    
    try:
        # Effectuer le merge (upsert)
        delta_table.alias("existing").merge(
            batch_df.alias("updates"),
            "existing.SchoolName = updates.SchoolName"
        ).whenMatchedCondition("updates.Timestamp > existing.Timestamp") \
         .whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        print(f"Batch {batch_id} : Delta Lake mis à jour.")
    except Exception as e:
        print(f"Erreur lors du merge pour le batch {batch_id}: {e}")

# 16. Définir le streaming query avec foreachBatch
query = updates_df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .outputMode("update") \
    .start()

query.awaitTermination()
