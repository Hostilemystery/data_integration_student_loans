# Exécution du projet

## Mettre ses configurations dans le fichier HDFS 

Data_cleaning/config/hdfs_config.py

Il est nécessaire de mettre le numéro de port de HDFS et les chemins vers les différents éléments où vous souhaitez stocker les éléments

## Installation de Kafka

Nous avons choisi d'utiliser Kafka avec Docker, vous avez juste à vous rendre dans le répertoire Kafka-installation et d'éxécuter `docker compose up`

## Data cleaning

Pour lancer le nettoyage des données exécuter ce notebook : Data_cleaning/Data_cleaning.ipynb

En précisant les chemins vers les données brutes et vers un répertoire pour sortir les données nettoyées

## Upload vers HDFS

Ensuite exécuter le fichier : Data_cleaning/Upload_hdfs.ipynb

En précisant les chemins vers les données nettoyées, vers un répertoire local pour stocker le fichier parquet et vers un répertoire sur HDFS

## Données en Streaming

Lancer d'abord le fichier Streaming/spark structured streaming.py qui attendra de recevoir des données du producer. Il faut adapter les chemins vers HDFS pour le stockage des dataframes et des checkpoint

Lancer le producer Streaming/kafka_producer.py qui enverra les données à Spark Streaming et adapter le numéro de port et le chemin du fichier Excel

## Jointure

Exécuter le fichier Jointure/join.ipynb en adaptant les chemins

source_path = chemin vers le fichier principal

backup_dir = chemin vers lequel stocker les backup

dataframes_path = chemin vers les fichiers générés par Spark Streaming

deuxième source_path = chemin vers lequel sera stockée la jointure
deuxième backup_dir = chemin vers lequel sera stocké la sauvegarde du fichier de jointure si un fichier est déjà présent

output_path = même chemin que le deuxième source_path

## Création des métriques

Exécuter le fichier Métriques/metrics.ipynb en changeant le chemin hdfs_parquet_path vers le chemin contenant le fichier après la jointure et le path dans chaque fonction pour stocker chaque métrique et faire un backup de chaque métrique

## Visualisation

Exécuter le fichier Métriques/visualisation.ipynb en adaptant les chemins vers les métriques

Si vous rencontrer des difficultés lors de l'éxécution des visualisations, vous pouvez créer un environnement virtuel et utiliser les packages du fichier requierements visualisation.txt