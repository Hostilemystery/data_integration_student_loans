from pyspark.sql import SparkSession

# Fonction permettant de liche des fichiers parquet sur HDFS pour s'assurer qu'ils ont bien été crées

def read_parquet():
    spark = SparkSession.builder \
        .appName("ReadParquet") \
        .getOrCreate()
    
    #hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/dfparquet"
    #hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/dataframes"
    #hdfs_parquet_path = "hdfs://localhost:9080/user/anthonycormeaux/data/jointest"
    hdfs_parquet_path = "hdfs://localhost:9080//user/anthonycormeaux/data/result/joined_data"

    df = spark.read.parquet(hdfs_parquet_path)

    df.show(10)
    
    df.printSchema()
    
    spark.stop()

if __name__ == "__main__":
    read_parquet()
