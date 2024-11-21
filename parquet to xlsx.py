from pyspark.sql import SparkSession
import pandas as pd

def parquet_to_csv(parquet_input_path, csv_output_path):
    """
    Convertit un fichier Parquet en fichier CSV sans passer par Pandas.

    :param parquet_input_path: Chemin vers le fichier Parquet d'entrée.
    :param csv_output_path: Chemin vers le répertoire CSV de sortie.
    """
    # Créer une session Spark
    spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

    # Lire le fichier Parquet
    df = spark.read.parquet(parquet_input_path)

    # Écrire le DataFrame en CSV
    df.write.mode('overwrite').option("header", "true").csv(csv_output_path)

    # Arrêter la session Spark
    spark.stop()



parquet_input = "hdfs://localhost:9080/user/anthonycormeaux/data/jointest"
excel_output = "//Users/anthonycormeaux/Documents/Projet_data_integration/Nouvelle version/data_integration_student_loans"

parquet_to_csv(parquet_input, excel_output)
