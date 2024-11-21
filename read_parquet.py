from pyspark.sql import SparkSession

def read_parquet():
    # Initialiser SparkSession
    spark = SparkSession.builder \
        .appName("ReadParquet") \
        .getOrCreate()
    
    # Définir le chemin HDFS où les fichiers Parquet sont stockés
    #hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/dfparquet"
    hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/dataframes"
    #hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/jointest"

    # Lire les fichiers Parquet en tant que DataFrame
    df = spark.read.parquet(hdfs_parquet_path)
    
    # Afficher les premières lignes du DataFrame
    #df.show(50)
    
    # Afficher le schéma du DataFrame
    df.printSchema()
    
    # Fermer SparkSession
    spark.stop()

if __name__ == "__main__":
    read_parquet()
