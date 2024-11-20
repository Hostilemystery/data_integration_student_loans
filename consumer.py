from kafka import KafkaConsumer
import json
from datetime import datetime

def json_deserializer(data):
    """
    Désérialise les données JSON reçues en dictionnaire Python.
    """
    try:
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Erreur de désérialisation JSON: {e}")
        return None  # Retourne None en cas d'erreur

def main():
    # Nom du topic auquel le consumer va s'abonner
    topic_name = 'excel_data'
    
    # Création du consumer Kafka
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],  # Adresse du serveur Kafka
        auto_offset_reset='earliest',          # Commence à lire depuis le début si aucun offset n'est enregistré
        enable_auto_commit=True,               # Active la validation automatique des offsets
        group_id='excel_data_group',           # Identifiant du groupe de consommateurs
        value_deserializer=json_deserializer   # Fonction de désérialisation des messages
    )
    
    print(f"Consommateur Kafka démarré et abonné au topic '{topic_name}'.")

    try:
        # Boucle infinie pour écouter les messages
        for message in consumer:
            # message.value contient le message désérialisé (dictionnaire Python) ou None en cas d'erreur
            data = message.value
            if data is None:
                print("Message ignoré en raison d'une erreur de désérialisation.")
                continue  # Passe au message suivant

            timestamp = datetime.fromtimestamp(data.get("timestamp", 0))
            print(f"\n--- Message Reçu ---")
            print(f"SchoolCode : {data.get('SchoolCode')}")
            print(f"SchoolName : {data.get('SchoolName')}")
            print(f"Address    : {data.get('Address')}")
            print(f"City       : {data.get('City')}")
            print(f"StateCode  : {data.get('StateCode')}")
            print(f"ZipCode    : {data.get('ZipCode')}")
            print(f"Country    : {data.get('Country')}")
            print(f"Timestamp  : {timestamp}")
            print("---------------------")
    except KeyboardInterrupt:
        print("\nArrêt du consommateur Kafka.")
    finally:
        # Fermeture propre du consumer
        consumer.close()

if __name__ == '__main__':
    main()
