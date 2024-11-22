from kafka import KafkaProducer
import json
import time
from datetime import datetime
import pandas as pd

# Chemin vers votre fichier Excel
excel_file_path = r"/Users/anthonycormeaux/Documents/Projet_data_integration/Nouvelle version/data_integration_student_loans/data/1617fedschoolcodelist.xls"

# Lire le fichier Excel
df = pd.read_excel(excel_file_path)

def json_serializer(data):
    """
    Sérialise les données en JSON encodé en UTF-8.
    """
    return json.dumps(data).encode('utf-8')

# Nom du topic Kafka
topic_name = 'excel_data'

# Taille du batch
batch_size = 100

# Temps de pause en secondes
sleep_time = 10

if __name__ == '__main__':
    # Initialiser le producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    total_rows = len(df)
    print(f"Nombre total de lignes à envoyer: {total_rows}")

    # Diviser le DataFrame en chunks de 100 lignes
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end]
        batch_number = (start // batch_size) + 1
        print(f"\nEnvoi du batch {batch_number}: lignes {start + 1} à {min(end, total_rows)}")

        for index, row in batch.iterrows():
            message = {
                "SchoolCode": row["SchoolCode"], 
                "SchoolName": row["SchoolName"],
                "Address": row["Address"],
                "City": row["City"],
                "StateCode" : row["StateCode"],
                "ZipCode" : row["ZipCode"],
                "Country" : row["Country"],  # Clé unique
                "timestamp": datetime.now().timestamp()
            }
            producer.send(topic_name, value=message)
            print(f"Message envoyé pour {row['SchoolCode']} (Batch {batch_number})")

        # Envoyer les messages en attente
        producer.flush()
        print(f"Batch {batch_number} envoyé. Pause de {sleep_time} secondes.")
        time.sleep(sleep_time)

    # Fermer le producteur Kafka après l'envoi de tous les batches
    producer.close()
    print("\nTous les messages ont été envoyés et le producteur Kafka est fermé.")
