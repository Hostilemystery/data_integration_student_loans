from pyspark.sql import SparkSession
import os

# Initialiser la session Spark
spark = SparkSession.builder.appName("JointureFichiers").getOrCreate()

# Chemins des fichiers sur HDFS
chemin_fichier_original = "hdfs://localhost:9080/user/anthonycormeaux/data/dfparquet"
chemin_sauvegarde = "hdfs://localhost:9080/user/anthonycormeaux/data/backup"
repertoire_nouveaux_fichiers = "hdfs://localhost:9080/user/anthonycormeaux/data/dataframes"

# Étape 1 : Sauvegarder le fichier original
os.system(f"hdfs dfs -cp {chemin_fichier_original} {chemin_sauvegarde}")

# Étape 2 : Lire le fichier original
df_original = spark.read.parquet(chemin_fichier_original)

# Étape 3 : Lire tous les nouveaux fichiers du répertoire
df_nouveaux = spark.read.parquet(repertoire_nouveaux_fichiers)

df_nouveaux = df_nouveaux.withColumnRenamed("SchoolName", "New_SchoolName")
df_nouveaux = df_nouveaux.withColumnRenamed("timestamp", "New_timestamp")

# Étape 4 : Effectuer la jointure sur les colonnes correspondantes
df_joint = df_original.join(
    df_nouveaux,
    df_original["OPE ID"] == df_nouveaux["SchoolCode"],
    how="left"
)

# Optionnel : Gérer les colonnes en double si nécessaire
# Par exemple, si vous voulez renommer la colonne 'SchoolName' du nouveau fichier
# df_joint = df_joint.withColumnRenamed("SchoolName", "New_SchoolName")

# Étape 5 : Enregistrer le nouveau fichier en remplaçant l'ancien
df_joint.write.mode("overwrite").parquet(chemin_fichier_original)

# Arrêter la session Spark
spark.stop()
